package store

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/robertramos/developer-observability-stack/internal/logs"
	"github.com/robertramos/developer-observability-stack/internal/logs/query"
)

func TestMaxActiveSegmentsSealsLeastRecentlyWritten(t *testing.T) {
	s := openTest(t, t.TempDir(), func(o *Options) { o.MaxActiveSegments = 2 })
	defer s.Close()
	for h := 0; h < 4; h++ { // four partitions written in order
		if err := s.Append(genEntries(3, base.Add(time.Duration(h)*time.Hour), time.Second)); err != nil {
			t.Fatal(err)
		}
		time.Sleep(2 * time.Millisecond)
	}
	if st := s.Stats(); st.ActiveSegments != 2 || st.Segments != 4 {
		t.Fatalf("stats %+v", st)
	}
	idx, _ := filepath.Glob(filepath.Join(s.opts.Dir, "*", "*.idx"))
	if len(idx) != 2 {
		t.Fatalf("sealed segments must have an index file: %v", idx)
	}
}

func TestFieldsAndPrunedPartitions(t *testing.T) {
	s := openTest(t, t.TempDir(), nil)
	defer s.Close()
	s.Append(genEntries(120, base, time.Minute)) // two partitions
	fields := s.Fields()
	names := map[string]int{}
	for _, f := range fields {
		names[f.Name] = f.MaxCardinality
	}
	if names["service"] != 3 || names["level"] != 3 || names["seq"] != 60 {
		t.Fatalf("fields %v", names)
	}
	q, _ := query.Parse("service:api")
	res, err := s.Search(context.Background(), SearchRequest{Query: q, Start: base.Add(90 * time.Minute).UnixNano(), Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if res.Stats.PartitionsScanned != 1 || len(res.Entries) != 10 {
		t.Fatalf("partition pruning by time: %+v", res.Stats)
	}
}

func TestRemovedSegmentSurvivesActiveReader(t *testing.T) {
	s := openTest(t, t.TempDir(), nil)
	defer s.Close()
	s.Append(genEntries(10, base, time.Second))
	refs, _ := s.snapshot(0, 1<<62)
	if len(refs) != 1 {
		t.Fatalf("refs %d", len(refs))
	}
	if n, _ := s.ApplyRetention(base.Add(2 * time.Hour).UnixNano()); n != 1 {
		t.Fatal("retention should drop the partition")
	}
	seg := refs[0].seg
	seg.flush()
	seg.mu.RLock()
	e, err := seg.readAt(3)
	seg.mu.RUnlock()
	if err != nil || e.Fields["seq"] != "3" {
		t.Fatalf("pinned segment unreadable: %v", err)
	}
	if seg.acquire() {
		t.Fatal("removed segments cannot be newly acquired")
	}
	releaseAll(refs)
	if _, err := os.Stat(seg.base + ".log"); !os.IsNotExist(err) {
		t.Fatal("files must be deleted after the last release")
	}
}

func TestCodecRoundTripAndCorruption(t *testing.T) {
	e := logs.Entry{Timestamp: -5, Level: "warn", Message: "héllo\x00world", Fields: map[string]string{"b": "2", "a": ""}}
	payload := encodeEntry(nil, &e)
	got, err := decodeEntry(payload)
	if err != nil || got.Message != e.Message || got.Timestamp != -5 || got.Fields["b"] != "2" || len(got.Fields) != 2 {
		t.Fatalf("round trip %+v %v", got, err)
	}
	for i := 1; i < len(payload); i++ {
		decodeEntry(payload[:i]) // must not panic on truncation
	}
	framed := frame(nil, payload)
	r := bufio.NewReader(bytes.NewReader(framed))
	p, n, err := readFrame(r, nil)
	if err != nil || n != len(framed) || !bytes.Equal(p, payload) {
		t.Fatalf("frame round trip %v", err)
	}
	if _, _, err := readFrame(bufio.NewReader(bytes.NewReader(nil)), nil); err != io.EOF {
		t.Fatal("empty stream is a clean EOF")
	}
	framed[len(framed)-1] ^= 1
	if _, _, err := readFrame(bufio.NewReader(bytes.NewReader(framed)), nil); err != errCorrupt {
		t.Fatal("crc mismatch must be detected")
	}
	if _, err := unmarshalIndex([]byte("OBSLIDX1garbage-without-valid-crc")); err == nil {
		t.Fatal("corrupt index must fail")
	}
}
