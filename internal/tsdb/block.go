package tsdb

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"time"

	"github.com/robertramos/developer-observability-stack/internal/tsdb/chunk"
	"github.com/robertramos/developer-observability-stack/internal/tsdb/labels"
)

// Aggregates stored per downsampled bucket.
const (
	AggrRaw   = "raw"
	AggrCount = "count"
	AggrSum   = "sum"
	AggrMin   = "min"
	AggrMax   = "max"
	AggrLast  = "last"
)

// DownsampleAggregates is the aggregate order inside downsampled blocks.
var DownsampleAggregates = []string{AggrCount, AggrSum, AggrMin, AggrMax, AggrLast}

// BlockMeta describes a persisted block. MaxT is exclusive.
type BlockMeta struct {
	ID         string    `json:"id"`
	MinT       int64     `json:"min_t"`
	MaxT       int64     `json:"max_t"`
	Resolution int64     `json:"resolution_ms"`
	Aggregates []string  `json:"aggregates"`
	Source     string    `json:"source,omitempty"`
	NumSeries  int       `json:"num_series"`
	NumSamples int       `json:"num_samples"`
	NumChunks  int       `json:"num_chunks"`
	SizeBytes  int64     `json:"size_bytes"`
	CreatedAt  time.Time `json:"created_at"`
}

type blockChunk struct {
	minT, maxT int64
	c          *chunk.XOR
}

type blockSeries struct {
	lset  labels.Labels
	aggrs [][]blockChunk // indexed like BlockMeta.Aggregates
}

// Block is an immutable, fully loaded on-disk block.
type Block struct {
	meta     BlockMeta
	dir      string
	series   []blockSeries
	postings map[string]map[string][]uint64
}

// Meta returns the block metadata.
func (b *Block) Meta() BlockMeta { return b.meta }

const blockMagic = "OBSBLK01"

func blockID(resolution, minT, maxT int64) string {
	if resolution == 0 {
		return fmt.Sprintf("raw-%d-%d", minT, maxT)
	}
	return fmt.Sprintf("ds%d-%d-%d", resolution, minT, maxT)
}

// writeBlock persists series into parent/<meta.ID> atomically (temp dir +
// rename) and returns the loaded block.
func writeBlock(parent string, meta BlockMeta, series []blockSeries) (*Block, error) {
	b := []byte(blockMagic)
	b = binary.AppendUvarint(b, uint64(len(series)))
	meta.NumSeries, meta.NumSamples, meta.NumChunks = len(series), 0, 0
	for _, s := range series {
		if len(s.aggrs) != len(meta.Aggregates) {
			return nil, fmt.Errorf("series %s has %d aggregates, want %d", s.lset, len(s.aggrs), len(meta.Aggregates))
		}
		b = binary.AppendUvarint(b, uint64(len(s.lset)))
		for _, l := range s.lset {
			b = appendStr(b, l.Name)
			b = appendStr(b, l.Value)
		}
		for ai, chunks := range s.aggrs {
			b = binary.AppendUvarint(b, uint64(len(chunks)))
			for _, c := range chunks {
				b = binary.AppendVarint(b, c.minT)
				b = binary.AppendVarint(b, c.maxT)
				raw := c.c.Bytes()
				b = binary.AppendUvarint(b, uint64(len(raw)))
				b = append(b, raw...)
				meta.NumChunks++
				if ai == 0 {
					meta.NumSamples += c.c.NumSamples()
				}
			}
		}
	}
	b = binary.LittleEndian.AppendUint32(b, crc32.Checksum(b, castagnoli))
	meta.SizeBytes = int64(len(b))
	if meta.CreatedAt.IsZero() {
		meta.CreatedAt = time.Now().UTC()
	}
	final := filepath.Join(parent, meta.ID)
	tmp := final + ".tmp"
	os.RemoveAll(tmp)
	if err := os.MkdirAll(tmp, 0o755); err != nil {
		return nil, err
	}
	metaJSON, _ := json.MarshalIndent(meta, "", "  ")
	if err := writeSynced(filepath.Join(tmp, "chunks.bin"), b); err != nil {
		return nil, err
	}
	if err := writeSynced(filepath.Join(tmp, "meta.json"), metaJSON); err != nil {
		return nil, err
	}
	if err := os.RemoveAll(final); err != nil {
		return nil, err
	}
	if err := os.Rename(tmp, final); err != nil {
		return nil, err
	}
	blk := &Block{meta: meta, dir: final, series: series}
	blk.buildPostings()
	return blk, nil
}

func writeSynced(path string, data []byte) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	_, werr := f.Write(data)
	serr := f.Sync()
	cerr := f.Close()
	return errors.Join(werr, serr, cerr)
}

// openBlock loads a block directory.
func openBlock(dir string) (*Block, error) {
	metaRaw, err := os.ReadFile(filepath.Join(dir, "meta.json"))
	if err != nil {
		return nil, err
	}
	var meta BlockMeta
	if err := json.Unmarshal(metaRaw, &meta); err != nil {
		return nil, fmt.Errorf("block meta: %w", err)
	}
	raw, err := os.ReadFile(filepath.Join(dir, "chunks.bin"))
	if err != nil {
		return nil, err
	}
	if len(raw) < len(blockMagic)+4 || string(raw[:len(blockMagic)]) != blockMagic {
		return nil, errors.New("block: bad magic")
	}
	body := raw[:len(raw)-4]
	if crc32.Checksum(body, castagnoli) != binary.LittleEndian.Uint32(raw[len(raw)-4:]) {
		return nil, errors.New("block: checksum mismatch")
	}
	d := &byteDecoder{b: body[len(blockMagic):]}
	n := d.uvarint()
	if n > uint64(len(body)) {
		return nil, errors.New("block: bad series count")
	}
	series := make([]blockSeries, 0, n)
	for i := uint64(0); i < n && d.err == nil; i++ {
		nl := d.uvarint()
		if nl > labels.MaxLabels+1 {
			return nil, errors.New("block: too many labels")
		}
		ls := make(labels.Labels, 0, nl)
		for j := uint64(0); j < nl && d.err == nil; j++ {
			ls = append(ls, labels.Label{Name: d.str(), Value: d.str()})
		}
		s := blockSeries{lset: ls, aggrs: make([][]blockChunk, len(meta.Aggregates))}
		for a := range meta.Aggregates {
			nc := d.uvarint()
			if nc > uint64(len(body)) {
				return nil, errors.New("block: bad chunk count")
			}
			for k := uint64(0); k < nc && d.err == nil; k++ {
				minT, maxT := d.varint(), d.varint()
				cb := d.bytes()
				if d.err != nil {
					break
				}
				c, err := chunk.FromBytes(cb)
				if err != nil {
					return nil, err
				}
				s.aggrs[a] = append(s.aggrs[a], blockChunk{minT, maxT, c})
			}
		}
		series = append(series, s)
	}
	if d.err != nil {
		return nil, fmt.Errorf("block %s: %w", dir, d.err)
	}
	b := &Block{meta: meta, dir: dir, series: series}
	b.buildPostings()
	return b, nil
}

func (b *Block) buildPostings() {
	b.postings = map[string]map[string][]uint64{}
	for i, s := range b.series {
		for _, l := range s.lset {
			vals := b.postings[l.Name]
			if vals == nil {
				vals = map[string][]uint64{}
				b.postings[l.Name] = vals
			}
			vals[l.Value] = append(vals[l.Value], uint64(i))
		}
	}
}

func (b *Block) aggrIndex(name string) int {
	for i, a := range b.meta.Aggregates {
		if a == name {
			return i
		}
	}
	return -1
}

// selectSeries returns matching series' samples of the given aggregate.
func (b *Block) selectSeries(ms []*labels.Matcher, mint, maxt int64, aggr string) []Series {
	ai := b.aggrIndex(aggr)
	if ai < 0 || maxt < b.meta.MinT || mint >= b.meta.MaxT {
		return nil
	}
	refs := postingsForMatchers(b.postings, func() []uint64 {
		all := make([]uint64, len(b.series))
		for i := range all {
			all[i] = uint64(i)
		}
		return all
	}, ms)
	var out []Series
	for _, r := range refs {
		s := &b.series[r]
		if !labels.MatchesAll(s.lset, ms) {
			continue
		}
		var smp []Sample
		for _, c := range s.aggrs[ai] {
			if c.maxT < mint || c.minT > maxt {
				continue
			}
			smp = appendChunkSamples(smp, c.c, mint, maxt)
		}
		if len(smp) > 0 {
			out = append(out, Series{Labels: s.lset, Samples: smp})
		}
	}
	return out
}

// labelValues adds the values of name to set.
func (b *Block) labelValues(name string, set map[string]struct{}) {
	for v := range b.postings[name] {
		set[v] = struct{}{}
	}
}

func (b *Block) labelNames(set map[string]struct{}) {
	for n := range b.postings {
		set[n] = struct{}{}
	}
}

// encodeChunks compresses samples into chunks of at most maxSamples.
func encodeChunks(samples []Sample, maxSamples int) ([]blockChunk, error) {
	var out []blockChunk
	for len(samples) > 0 {
		n := min(maxSamples, len(samples))
		c := chunk.NewXOR()
		app, err := c.Appender()
		if err != nil {
			return nil, err
		}
		for _, s := range samples[:n] {
			if err := app.Append(s.T, s.V); err != nil {
				return nil, err
			}
		}
		out = append(out, blockChunk{minT: samples[0].T, maxT: samples[n-1].T, c: c})
		samples = samples[n:]
	}
	return out, nil
}
