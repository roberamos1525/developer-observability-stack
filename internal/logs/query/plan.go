package query

import (
	"unicode/utf8"

	"github.com/robertramos/developer-observability-stack/internal/logs/postings"
)

// MaxIndexedValueLen is the longest field value stored verbatim in the
// field index. Longer values are stored truncated with a NUL marker, so
// equality on them must be verified against the entry.
const MaxIndexedValueLen = 240

// IndexKey is the form of a field value stored in the field index. Both the
// index writer and the planner use it so they always agree.
func IndexKey(v string) string {
	if len(v) <= MaxIndexedValueLen {
		return v
	}
	n := MaxIndexedValueLen
	for n > 0 && !utf8.RuneStart(v[n]) {
		n--
	}
	return v[:n] + string(rune(0))
}

// Index is the inverted index of one segment, as seen by the planner.
type Index interface {
	Len() uint32
	Term(token string) postings.List
	TermPrefix(prefix string) postings.List
	Field(name, value string) postings.List
	FieldPrefix(name, prefix string) postings.List
}

// Candidates is the planner's output for one segment.
type Candidates struct {
	// All means every ordinal in the segment is a candidate.
	All bool
	// IDs holds the candidate ordinals when All is false.
	IDs postings.List
	// Exact means every candidate is known to match: no verification needed.
	Exact bool
}

// List materialises the candidate ordinals.
func (c Candidates) List(ix Index) postings.List {
	if c.All {
		return postings.Range(ix.Len())
	}
	return c.IDs
}

// Plan computes the candidate set for n against ix.
func Plan(n Node, ix Index) Candidates {
	switch x := n.(type) {
	case MatchAll:
		return Candidates{All: true, Exact: true}
	case *Term:
		if x.Prefix {
			return Candidates{IDs: ix.TermPrefix(x.Token), Exact: true}
		}
		return Candidates{IDs: ix.Term(x.Token), Exact: true}
	case *Phrase:
		var ids postings.List
		for i, t := range x.Tokens {
			l := ix.Term(t)
			if i == 0 {
				ids = l
			} else {
				ids = postings.Intersect(ids, l)
			}
			if len(ids) == 0 {
				return Candidates{Exact: true}
			}
		}
		return Candidates{IDs: ids, Exact: len(x.Tokens) == 1}
	case *FieldMatch:
		return planField(x, ix)
	case *And:
		l, r := Plan(x.L, ix), Plan(x.R, ix)
		exact := l.Exact && r.Exact
		switch {
		case l.All && r.All:
			return Candidates{All: true, Exact: exact}
		case l.All:
			return Candidates{IDs: r.IDs, Exact: exact}
		case r.All:
			return Candidates{IDs: l.IDs, Exact: exact}
		}
		ids := postings.Intersect(l.IDs, r.IDs)
		if len(ids) == 0 && exact {
			return Candidates{Exact: true}
		}
		return Candidates{IDs: ids, Exact: exact || len(ids) == 0}
	case *Or:
		l, r := Plan(x.L, ix), Plan(x.R, ix)
		if l.All || r.All {
			// An exact "all" side matches everything, so the union is exact.
			return Candidates{All: true, Exact: l.All && l.Exact || r.All && r.Exact}
		}
		return Candidates{IDs: postings.Union(l.IDs, r.IDs), Exact: l.Exact && r.Exact}
	case *Not:
		c := Plan(x.X, ix)
		if !c.Exact {
			return Candidates{All: true}
		}
		if c.All {
			return Candidates{Exact: true}
		}
		return Candidates{IDs: postings.Difference(postings.Range(ix.Len()), c.IDs), Exact: true}
	}
	return Candidates{All: true}
}

func planField(x *FieldMatch, ix Index) Candidates {
	switch x.Op {
	case OpEq, OpNeq:
	default:
		return Candidates{All: true}
	}
	if x.Prefix && len(x.Value) > MaxIndexedValueLen-utf8.UTFMax || !x.Prefix && len(x.Value) > MaxIndexedValueLen {
		return Candidates{All: true}
	}
	var ids postings.List
	if x.Prefix {
		ids = ix.FieldPrefix(x.Field, x.Value)
	} else {
		ids = ix.Field(x.Field, x.Value)
	}
	if x.Op == OpEq {
		return Candidates{IDs: ids, Exact: true}
	}
	return Candidates{IDs: postings.Difference(postings.Range(ix.Len()), ids), Exact: true}
}
