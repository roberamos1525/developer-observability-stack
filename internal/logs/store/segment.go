package store

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/robertramos/developer-observability-stack/internal/logs"
)

// segment is one append-only log file plus its index. Active segments
// accept appends; sealed segments are immutable and have a persisted .idx.
type segment struct {
	id   uint64
	base string // path without extension

	mu     sync.RWMutex
	wf     *os.File
	w      *bufio.Writer
	dirty  bool
	size   int64
	rf     *os.File
	ix     *segIndex
	sealed bool

	refs    atomic.Int32
	removed atomic.Bool
	closeMu sync.Mutex
	closed  bool
}

func segBase(dir string, id uint64) string { return filepath.Join(dir, fmt.Sprintf("%016x", id)) }

func createSegment(dir string, id uint64) (*segment, error) {
	base := segBase(dir, id)
	wf, err := os.OpenFile(base+".log", os.O_CREATE|os.O_WRONLY|os.O_APPEND|os.O_EXCL, 0o644)
	if err != nil {
		return nil, err
	}
	rf, err := os.Open(base + ".log")
	if err != nil {
		wf.Close()
		return nil, err
	}
	return &segment{id: id, base: base, wf: wf, w: bufio.NewWriterSize(wf, 64<<10), rf: rf, ix: newSegIndex()}, nil
}

// openSegment opens an existing segment. If its index is missing or
// corrupt the log is scanned, any torn tail truncated, and the index
// rebuilt and persisted.
func openSegment(dir string, id uint64) (seg *segment, recovered bool, err error) {
	base := segBase(dir, id)
	if raw, err := os.ReadFile(base + ".idx"); err == nil {
		if ix, err := unmarshalIndex(raw); err == nil {
			rf, err := os.Open(base + ".log")
			if err != nil {
				return nil, false, err
			}
			st, err := rf.Stat()
			if err != nil {
				rf.Close()
				return nil, false, err
			}
			return &segment{id: id, base: base, rf: rf, ix: ix, sealed: true, size: st.Size()}, false, nil
		}
	}
	ix, size, err := scanLog(base + ".log")
	if err != nil {
		return nil, false, err
	}
	if err := writeFileAtomic(base+".idx", ix.marshal()); err != nil {
		return nil, false, err
	}
	ix.freeze()
	rf, err := os.Open(base + ".log")
	if err != nil {
		return nil, false, err
	}
	return &segment{id: id, base: base, rf: rf, ix: ix, sealed: true, size: size}, true, nil
}

// scanLog rebuilds an index from a log file, truncating any corrupt tail.
func scanLog(path string) (*segIndex, int64, error) {
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return nil, 0, err
	}
	defer f.Close()
	ix := newSegIndex()
	r := bufio.NewReaderSize(f, 256<<10)
	var off int64
	var buf []byte
	for {
		payload, n, err := readFrame(r, buf)
		if err == io.EOF {
			break
		}
		if err != nil {
			break // torn or corrupt tail: everything after off is discarded
		}
		e, err := decodeEntry(payload)
		if err != nil {
			break
		}
		ix.add(&e, off)
		off += int64(n)
		buf = payload[:0]
	}
	st, err := f.Stat()
	if err != nil {
		return nil, 0, err
	}
	if st.Size() != off {
		if err := f.Truncate(off); err != nil {
			return nil, 0, err
		}
		if err := f.Sync(); err != nil {
			return nil, 0, err
		}
	}
	return ix, off, nil
}

func (s *segment) append(e *logs.Entry, scratch []byte) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sealed {
		return scratch, errors.New("logstore: append to sealed segment")
	}
	payload := encodeEntry(scratch[:0], e)
	rec := frame(make([]byte, 0, len(payload)+16), payload)
	if _, err := s.w.Write(rec); err != nil {
		return payload, err
	}
	s.ix.add(e, s.size)
	s.size += int64(len(rec))
	s.dirty = true
	return payload, nil
}

func (s *segment) flush() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.flushLocked()
}

func (s *segment) flushLocked() error {
	if !s.dirty || s.w == nil {
		return nil
	}
	if err := s.w.Flush(); err != nil {
		return err
	}
	s.dirty = false
	return nil
}

// seal flushes, fsyncs and persists the index; the segment becomes immutable.
func (s *segment) seal() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sealed {
		return nil
	}
	if err := s.flushLocked(); err != nil {
		return err
	}
	if err := s.wf.Sync(); err != nil {
		return err
	}
	if err := writeFileAtomic(s.base+".idx", s.ix.marshal()); err != nil {
		return err
	}
	if err := s.wf.Close(); err != nil {
		return err
	}
	s.wf, s.w = nil, nil
	s.ix.freeze()
	s.sealed = true
	return nil
}

func (s *segment) stats() (entries uint32, size int64, minTs, maxTs int64, sealed bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.ix.n, s.size, s.ix.minTs, s.ix.maxTs, s.sealed
}

// readAt decodes the entry with ordinal id. Caller holds s.mu (read).
func (s *segment) readAt(id uint32) (logs.Entry, error) {
	off := s.ix.offsets[id]
	var hdr [binary.MaxVarintLen64]byte
	n, err := s.rf.ReadAt(hdr[:], off)
	if n == 0 && err != nil {
		return logs.Entry{}, err
	}
	l, k := binary.Uvarint(hdr[:n])
	if k <= 0 || l > maxRecordSize {
		return logs.Entry{}, errCorrupt
	}
	buf := make([]byte, int(l)+4)
	if _, err := s.rf.ReadAt(buf, off+int64(k)); err != nil {
		return logs.Entry{}, err
	}
	if crc32.Checksum(buf[:l], castagnoli) != binary.LittleEndian.Uint32(buf[l:]) {
		return logs.Entry{}, errCorrupt
	}
	return decodeEntry(buf[:l])
}

// acquire pins the segment against removal; it fails if already removed.
func (s *segment) acquire() bool {
	s.refs.Add(1)
	if s.removed.Load() {
		s.release()
		return false
	}
	return true
}

func (s *segment) release() {
	if s.refs.Add(-1) == 0 && s.removed.Load() {
		s.destroy()
	}
}

// remove marks the segment deleted; files are closed and unlinked once the
// last reader releases it.
func (s *segment) remove() {
	if s.removed.Swap(true) {
		return
	}
	if s.refs.Load() == 0 {
		s.destroy()
	}
}

func (s *segment) destroy() {
	s.closeFiles()
	os.Remove(s.base + ".idx")
	os.Remove(s.base + ".log")
	// Removes the partition directory once retention emptied it; harmless
	// failure when other segments remain.
	os.Remove(filepath.Dir(s.base))
}

func (s *segment) closeFiles() error {
	s.closeMu.Lock()
	defer s.closeMu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	s.mu.Lock()
	defer s.mu.Unlock()
	var errs []error
	if s.w != nil {
		errs = append(errs, s.w.Flush())
	}
	if s.wf != nil {
		errs = append(errs, s.wf.Sync(), s.wf.Close())
	}
	if s.rf != nil {
		errs = append(errs, s.rf.Close())
	}
	return errors.Join(errs...)
}
