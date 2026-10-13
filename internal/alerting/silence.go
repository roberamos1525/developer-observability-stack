package alerting

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"regexp"
	"time"
)

// Matcher selects alerts by label.
type Matcher struct {
	Name    string `json:"name"`
	Value   string `json:"value"`
	IsRegex bool   `json:"isRegex,omitempty"`
	IsEqual *bool  `json:"isEqual,omitempty"` // default true
	re      *regexp.Regexp
}

func (m *Matcher) compile() error {
	if m.Name == "" {
		return fmt.Errorf("matcher name is required")
	}
	if m.IsRegex {
		re, err := regexp.Compile("^(?:" + m.Value + ")$")
		if err != nil {
			return fmt.Errorf("matcher %s: %w", m.Name, err)
		}
		m.re = re
	}
	return nil
}

func (m *Matcher) matches(ls map[string]string) bool {
	v := ls[m.Name]
	var ok bool
	if m.IsRegex {
		ok = m.re.MatchString(v)
	} else {
		ok = v == m.Value
	}
	if m.IsEqual != nil && !*m.IsEqual {
		return !ok
	}
	return ok
}

// Silence mutes notifications for matching alerts during [StartsAt, EndsAt).
type Silence struct {
	ID        string    `json:"id"`
	Matchers  []Matcher `json:"matchers"`
	StartsAt  time.Time `json:"startsAt"`
	EndsAt    time.Time `json:"endsAt"`
	CreatedBy string    `json:"createdBy"`
	Comment   string    `json:"comment"`
}

// Validate checks and compiles the silence.
func (s *Silence) Validate() error {
	if len(s.Matchers) == 0 || len(s.Matchers) > 32 {
		return fmt.Errorf("a silence needs 1..32 matchers")
	}
	allEmpty := true
	for i := range s.Matchers {
		if err := s.Matchers[i].compile(); err != nil {
			return err
		}
		if s.Matchers[i].matches(map[string]string{}) == false {
			allEmpty = false
		}
	}
	if allEmpty {
		return fmt.Errorf("matchers would silence every alert; at least one must not match empty labels")
	}
	if !s.EndsAt.After(s.StartsAt) {
		return fmt.Errorf("endsAt must be after startsAt")
	}
	if s.CreatedBy == "" || len(s.Comment) > 1024 {
		return fmt.Errorf("createdBy is required and comment is limited to 1024 bytes")
	}
	return nil
}

// Active reports whether the silence applies at now.
func (s *Silence) Active(now time.Time) bool {
	return !now.Before(s.StartsAt) && now.Before(s.EndsAt)
}

// Status is "pending", "active" or "expired".
func (s *Silence) Status(now time.Time) string {
	switch {
	case now.Before(s.StartsAt):
		return "pending"
	case now.Before(s.EndsAt):
		return "active"
	}
	return "expired"
}

// Matches reports whether all matchers match the label set.
func (s *Silence) Matches(ls map[string]string) bool {
	for i := range s.Matchers {
		if !s.Matchers[i].matches(ls) {
			return false
		}
	}
	return true
}

func newID() string {
	var b [12]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b[:])
}
