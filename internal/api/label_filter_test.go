package api

import (
	"net/http"
	"reflect"
	"testing"
)

// TestLabelFilterFromQuery guards the label=key=value query-param parsing
// (F-29 / F-21.7): repeated params accumulate, a value may itself contain '='
// (cut at the FIRST separator), an absent filter yields nil, a malformed
// param is an error, and the LEGACY tag= param is not recognized (the F-21.7
// rename deliberately maintains no backwards compatibility). Regression: the
// rename initially left this reading "tag", silently disabling the filter.
func TestLabelFilterFromQuery(t *testing.T) {
	newReq := func(rawQuery string) *http.Request {
		r, err := http.NewRequest(http.MethodGet, "http://localhost"+rawQuery, nil)
		if err != nil {
			t.Fatalf("NewRequest(%q): %v", rawQuery, err)
		}
		return r
	}

	t.Run("two labels", func(t *testing.T) {
		got, err := labelFilterFromQuery(newReq("/api/pipelines?label=team=backend&label=cost=1234"))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		want := map[string]string{"team": "backend", "cost": "1234"}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
	})

	t.Run("value containing =", func(t *testing.T) {
		got, err := labelFilterFromQuery(newReq("/api/pipelines?label=name=a=b"))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got["name"] != "a=b" {
			t.Errorf("got %v, want {name:a=b}", got)
		}
	})

	t.Run("absent filter", func(t *testing.T) {
		got, err := labelFilterFromQuery(newReq("/api/pipelines"))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got != nil {
			t.Errorf("got %v, want nil", got)
		}
	})

	t.Run("legacy tag param is not a label filter", func(t *testing.T) {
		got, err := labelFilterFromQuery(newReq("/api/pipelines?tag=team=backend"))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got != nil {
			t.Errorf("got %v, want nil (tag= is not recognized)", got)
		}
	})

	t.Run("param without equals is malformed", func(t *testing.T) {
		_, err := labelFilterFromQuery(newReq("/api/pipelines?label=noequals"))
		if err == nil {
			t.Error("expected an error for a param without '='")
		}
	})
}
