package postings

import (
	"math/rand"
	"reflect"
	"sort"
	"testing"
)

func randomList(rng *rand.Rand, n int, max uint32) List {
	set := map[uint32]bool{}
	for i := 0; i < n; i++ {
		set[uint32(rng.Intn(int(max)))] = true
	}
	out := make(List, 0, len(set))
	for v := range set {
		out = append(out, v)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func toSet(l List) map[uint32]bool {
	m := map[uint32]bool{}
	for _, v := range l {
		m[v] = true
	}
	return m
}

func TestSetAlgebraAgainstMaps(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	for iter := 0; iter < 200; iter++ {
		a := randomList(rng, rng.Intn(60), 100)
		b := randomList(rng, rng.Intn(60), 100)
		sa, sb := toSet(a), toSet(b)
		for _, v := range Intersect(a, b) {
			if !sa[v] || !sb[v] {
				t.Fatal("intersect produced foreign element")
			}
		}
		u := Union(a, b)
		if len(u) != len(toSet(append(append(List{}, a...), b...))) {
			t.Fatal("union size")
		}
		for _, v := range Difference(a, b) {
			if !sa[v] || sb[v] {
				t.Fatal("difference wrong")
			}
		}
		if len(Intersect(a, b))+len(Difference(a, b)) != len(a) {
			t.Fatal("partition identity violated")
		}
		for i := 1; i < len(u); i++ {
			if u[i] <= u[i-1] {
				t.Fatal("union not strictly increasing")
			}
		}
		for v := uint32(0); v < 100; v++ {
			if a.Contains(v) != sa[v] {
				t.Fatal("Contains mismatch")
			}
		}
	}
	if got := UnionAll([]List{{1, 5}, {2}, {5, 9}, nil}); !reflect.DeepEqual(got, List{1, 2, 5, 9}) {
		t.Fatalf("UnionAll %v", got)
	}
	if !reflect.DeepEqual(Range(3), List{0, 1, 2}) {
		t.Fatal("Range")
	}
}

func TestEncodeDecodeRoundTrip(t *testing.T) {
	rng := rand.New(rand.NewSource(2))
	for iter := 0; iter < 100; iter++ {
		l := randomList(rng, rng.Intn(500), 1<<31)
		buf := Encode([]byte{0xAA}, l)
		got, n, err := Decode(buf[1:])
		if err != nil || n != len(buf)-1 {
			t.Fatalf("decode: %v n=%d", err, n)
		}
		if len(l) == 0 && len(got) == 0 {
			continue
		}
		if !reflect.DeepEqual(got, l) {
			t.Fatal("round trip mismatch")
		}
	}
	for _, bad := range [][]byte{{}, {5, 1}, {2, 3, 0}, {0xff}} {
		if _, _, err := Decode(bad); err == nil {
			t.Errorf("Decode(%v) should fail", bad)
		}
	}
}
