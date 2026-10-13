package labels

import "testing"

func TestLabelsBasics(t *testing.T) {
	ls := FromStrings("job", "api", "__name__", "up", "instance", "a:1", "empty", "")
	if len(ls) != 3 || ls[0].Name != MetricName || ls.Get("job") != "api" || ls.Has("empty") {
		t.Fatalf("unexpected %v", ls)
	}
	if got := ls.String(); got != `up{instance="a:1", job="api"}` {
		t.Fatalf("String() = %s", got)
	}
	if !Equal(ls, ls.Copy()) || Equal(ls, ls.Without("job")) {
		t.Fatal("Equal/Copy/Without")
	}
	if got := ls.Only("job", "nope"); len(got) != 1 || got[0].Value != "api" {
		t.Fatalf("Only %v", got)
	}
	if ls.DropMetricName().Has(MetricName) || ls.With("job", "").Has("job") || ls.With("zone", "x").Get("zone") != "x" {
		t.Fatal("DropMetricName/With")
	}
	a := FromStrings("a", "b|c")
	b := FromStrings("a|b", "c")
	if a.Key() == b.Key() || a.Hash() == b.Hash() {
		t.Fatal("Key must be unambiguous")
	}
}

func TestValidate(t *testing.T) {
	good := FromStrings("__name__", "http:requests_total", "code", "200")
	if err := good.Validate(); err != nil {
		t.Fatal(err)
	}
	bad := []Labels{
		FromStrings("job", "x"),
		FromStrings("__name__", "1abc"),
		FromStrings("__name__", "ok", "bad-label", "x"),
		{{Name: "__name__", Value: "a"}, {Name: "z", Value: "1"}, {Name: "b", Value: "2"}},
	}
	for _, l := range bad {
		if l.Validate() == nil {
			t.Errorf("%v should be invalid", l)
		}
	}
}

func TestSanitizeName(t *testing.T) {
	cases := map[string]string{"http.server.duration": "http_server_duration", "9lives": "_9lives", "ok_name": "ok_name", "": "_", "a:b": "a_b"}
	for in, want := range cases {
		if got := SanitizeName(in, false); got != want {
			t.Errorf("SanitizeName(%q) = %q, want %q", in, got, want)
		}
	}
	if SanitizeName("a:b", true) != "a:b" {
		t.Error("metric names keep colons")
	}
}

func TestMatchers(t *testing.T) {
	re := MustMatcher(MatchRegexp, "path", "/api/.*")
	if !re.Matches("/api/v1") || re.Matches("x/api/v1") {
		t.Fatal("regex must be fully anchored")
	}
	nre := MustMatcher(MatchNotRegexp, "path", "/api/.*")
	if nre.Matches("/api/x") || !nre.Matches("") {
		t.Fatal("negative regex")
	}
	if _, err := NewMatcher(MatchRegexp, "x", "("); err == nil {
		t.Fatal("bad regex must fail")
	}
	ls := FromStrings("__name__", "up", "job", "api")
	if !MatchesAll(ls, []*Matcher{MustMatcher(MatchEqual, "job", "api"), MustMatcher(MatchEqual, "zone", "")}) {
		t.Fatal("missing label equals empty string")
	}
	if MatchesAll(ls, []*Matcher{MustMatcher(MatchNotEqual, "job", "api")}) {
		t.Fatal("not-equal")
	}
	if re.String() != `path=~"/api/.*"` {
		t.Fatal(re.String())
	}
}
