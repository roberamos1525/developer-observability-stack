package tsdb

import (
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"time"
)

// CutBlocks persists completed block-duration windows from the head into
// immutable blocks. A window [t, t+blockDur) is cut once the head extends
// at least half a block beyond its end, leaving room for slightly late
// samples. With force, every window up to the head's max time is cut
// (used by tests and offline tooling). Returns the number of blocks cut.
func (db *DB) CutBlocks(force bool) (int, error) {
	db.appendMu.Lock()
	defer db.appendMu.Unlock()
	cut := 0
	for {
		minT, maxT, ok := db.head.bounds()
		if !ok {
			return cut, nil
		}
		start := floorDiv(minT, db.blockDur) * db.blockDur
		end := start + db.blockDur
		if !force && maxT < end+db.blockDur/2 {
			return cut, nil
		}
		if force && start > maxT {
			return cut, nil
		}
		if err := db.cutWindow(start, end); err != nil {
			return cut, err
		}
		cut++
	}
}

// cutWindow writes head samples in [start,end) to a block, truncates the
// head and checkpoints the WAL. Caller holds appendMu.
func (db *DB) cutWindow(start, end int64) error {
	var series []blockSeries
	var err error
	db.head.forEach(func(s *memSeries) {
		if err != nil {
			return
		}
		smp := s.samples(start, end-1)
		if len(smp) == 0 {
			return
		}
		chunks, cerr := encodeChunks(smp, db.opts.ChunkMaxSamples)
		if cerr != nil {
			err = cerr
			return
		}
		series = append(series, blockSeries{lset: s.lset, aggrs: [][]blockChunk{chunks}})
	})
	if err != nil {
		return err
	}
	if len(series) > 0 {
		meta := BlockMeta{ID: blockID(0, start, end), MinT: start, MaxT: end, Aggregates: []string{AggrRaw}}
		b, err := writeBlock(filepath.Join(db.opts.Dir, "blocks"), meta, series)
		if err != nil {
			return fmt.Errorf("write block: %w", err)
		}
		db.mu.Lock()
		db.blocks = append(db.blocks, b)
		sortBlocks(db.blocks)
		db.mu.Unlock()
		db.opts.Logger.Info("cut metrics block", "id", meta.ID, "series", b.meta.NumSeries, "samples", b.meta.NumSamples)
	}
	db.head.truncateBefore(end)
	return db.checkpoint()
}

// checkpoint rewrites the WAL to contain only the live head.
func (db *DB) checkpoint() error {
	var series []walSeries
	var samples []walSample
	db.head.forEach(func(s *memSeries) {
		series = append(series, walSeries{s.ref, s.lset})
		for _, smp := range s.samples(math.MinInt64, math.MaxInt64) {
			samples = append(samples, walSample{s.ref, smp.T, smp.V})
		}
	})
	return db.wal.checkpoint(series, samples)
}

// Downsample creates downsampled blocks for raw blocks that ended before
// now-DownsampleAfter and have not been downsampled yet. Returns the
// number of blocks created.
func (db *DB) Downsample(ctx context.Context, now time.Time) (int, error) {
	res := db.opts.DownsampleResolution.Milliseconds()
	cutoff := now.Add(-db.opts.DownsampleAfter).UnixMilli()
	db.mu.RLock()
	done := map[string]bool{}
	for _, b := range db.dsBlocks {
		done[b.meta.Source] = true
	}
	var todo []*Block
	for _, b := range db.blocks {
		if b.meta.MaxT <= cutoff && !done[b.meta.ID] {
			todo = append(todo, b)
		}
	}
	db.mu.RUnlock()
	created := 0
	for _, src := range todo {
		if err := ctx.Err(); err != nil {
			return created, err
		}
		b, err := downsampleBlock(src, res, db.opts.ChunkMaxSamples, filepath.Join(db.opts.Dir, "blocks"))
		if err != nil {
			return created, err
		}
		db.mu.Lock()
		db.dsBlocks = append(db.dsBlocks, b)
		sortBlocks(db.dsBlocks)
		db.mu.Unlock()
		created++
	}
	return created, nil
}

// downsampleBlock aggregates each series of src into res-wide buckets
// holding count, sum, min, max and last. Each aggregate sample carries the
// timestamp of the last raw sample in its bucket.
func downsampleBlock(src *Block, res int64, maxSamples int, parent string) (*Block, error) {
	var series []blockSeries
	for _, s := range src.series {
		var raw []Sample
		for _, c := range s.aggrs[0] {
			raw = appendChunkSamples(raw, c.c, math.MinInt64, math.MaxInt64)
		}
		if len(raw) == 0 {
			continue
		}
		aggs := make([][]Sample, len(DownsampleAggregates))
		for i := 0; i < len(raw); {
			bucket := floorDiv(raw[i].T, res)
			j := i
			count, sum := 0.0, 0.0
			mn, mx := math.Inf(1), math.Inf(-1)
			for j < len(raw) && floorDiv(raw[j].T, res) == bucket {
				v := raw[j].V
				count++
				sum += v
				if v < mn || math.IsNaN(v) {
					mn = v
				}
				if v > mx || math.IsNaN(v) {
					mx = v
				}
				j++
			}
			last := raw[j-1]
			vals := []float64{count, sum, mn, mx, last.V}
			for a, v := range vals {
				aggs[a] = append(aggs[a], Sample{last.T, v})
			}
			i = j
		}
		bs := blockSeries{lset: s.lset, aggrs: make([][]blockChunk, len(aggs))}
		for a, smp := range aggs {
			chunks, err := encodeChunks(smp, maxSamples)
			if err != nil {
				return nil, err
			}
			bs.aggrs[a] = chunks
		}
		series = append(series, bs)
	}
	meta := BlockMeta{
		ID: blockID(res, src.meta.MinT, src.meta.MaxT), MinT: src.meta.MinT, MaxT: src.meta.MaxT,
		Resolution: res, Aggregates: DownsampleAggregates, Source: src.meta.ID,
	}
	return writeBlock(parent, meta, series)
}

// RetentionResult reports blocks deleted by ApplyRetention.
type RetentionResult struct {
	RawBlocks         int `json:"raw_blocks"`
	DownsampledBlocks int `json:"downsampled_blocks"`
	HeadSeries        int `json:"head_series"`
}

// ApplyRetention deletes raw blocks older than RawRetention, downsampled
// blocks older than DownsampledRetention, and head data older than the raw
// retention (which can only happen when blocks are never cut).
func (db *DB) ApplyRetention(now time.Time) (RetentionResult, error) {
	rawCut := now.Add(-db.opts.RawRetention).UnixMilli()
	dsCut := now.Add(-db.opts.DownsampledRetention).UnixMilli()
	var res RetentionResult
	var doomed []*Block
	db.mu.Lock()
	keep := db.blocks[:0]
	for _, b := range db.blocks {
		if b.meta.MaxT <= rawCut {
			doomed = append(doomed, b)
			res.RawBlocks++
		} else {
			keep = append(keep, b)
		}
	}
	db.blocks = keep
	keepDS := db.dsBlocks[:0]
	for _, b := range db.dsBlocks {
		if b.meta.MaxT <= dsCut {
			doomed = append(doomed, b)
			res.DownsampledBlocks++
		} else {
			keepDS = append(keepDS, b)
		}
	}
	db.dsBlocks = keepDS
	db.mu.Unlock()
	for _, b := range doomed {
		// Blocks are fully loaded in memory, so concurrent readers holding
		// a reference are unaffected by removing the directory.
		if err := os.RemoveAll(b.dir); err != nil {
			return res, err
		}
	}
	db.appendMu.Lock()
	if minT, _, ok := db.head.bounds(); ok && minT < rawCut {
		res.HeadSeries = db.head.truncateBefore(rawCut)
		if err := db.checkpoint(); err != nil {
			db.appendMu.Unlock()
			return res, err
		}
	}
	db.appendMu.Unlock()
	return res, nil
}
