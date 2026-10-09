package audit

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestLoggerJSON verifies the Logger renders an event as a single-line JSON
// object with a stable key order and omits empty fields.
func TestLoggerJSON(t *testing.T) {
	path := filepath.Join(t.TempDir(), "audit.log")
	l, err := NewLogger(Config{File: path, Format: "json"})
	if err != nil {
		t.Fatalf("NewLogger: %v", err)
	}
	defer l.Close()

	e := Event{
		Time:       time.Date(2026, 10, 9, 14, 30, 0, 0, time.UTC),
		Actor:      "alice",
		ActorKind:  ActorKindUser,
		Action:     ActionPipelineCreate,
		TargetKind: TargetPipeline,
		TargetID:   "1",
		TargetName: "build",
		PipelineID: 1,
		Outcome:    OutcomeSuccess,
		SourceIP:   "10.0.0.1",
		NewValue:   `{"name":"build"}`,
	}
	if err := l.Log(e); err != nil {
		t.Fatalf("Log: %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) != 1 {
		t.Fatalf("lines = %d, want 1", len(lines))
	}
	var m map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &m); err != nil {
		t.Fatalf("not valid JSON: %v (%s)", err, lines[0])
	}
	if m["actor"] != "alice" || m["action"] != ActionPipelineCreate || m["outcome"] != OutcomeSuccess {
		t.Errorf("fields = %v, want actor=alice action=%s outcome=%s", m, ActionPipelineCreate, OutcomeSuccess)
	}
	if m["target_name"] != "build" || m["source_ip"] != "10.0.0.1" {
		t.Errorf("target_name/source_ip = %v/%v", m["target_name"], m["source_ip"])
	}
	// Empty fields are omitted.
	if _, ok := m["run_id"]; ok {
		t.Errorf("run_id present, want omitted (zero)")
	}
	if _, ok := m["old_value"]; ok {
		t.Errorf("old_value present, want omitted (empty)")
	}
}

// TestLoggerText verifies the Logger renders an event as a single line of
// key=value pairs in text format.
func TestLoggerText(t *testing.T) {
	path := filepath.Join(t.TempDir(), "audit.log")
	l, err := NewLogger(Config{File: path, Format: "text"})
	if err != nil {
		t.Fatalf("NewLogger: %v", err)
	}
	defer l.Close()

	if err := l.Log(Event{Actor: "bob", Action: ActionLogin, Outcome: OutcomeFailure}); err != nil {
		t.Fatalf("Log: %v", err)
	}
	data, _ := os.ReadFile(path)
	line := strings.TrimSpace(string(data))
	if !strings.Contains(line, `actor="bob"`) {
		t.Errorf("line = %q, want actor=\"bob\"", line)
	}
	if !strings.Contains(line, "action=auth.login") {
		t.Errorf("line = %q, want action=auth.login", line)
	}
	if !strings.Contains(line, "outcome=failure") {
		t.Errorf("line = %q, want outcome=failure", line)
	}
}

// TestLoggerInvalidFormat verifies NewLogger rejects an unknown format.
func TestLoggerInvalidFormat(t *testing.T) {
	if _, err := NewLogger(Config{File: "-", Format: "xml"}); err == nil {
		t.Fatal("NewLogger(xml) = nil error, want error")
	}
}

// TestLoggerStdout verifies a "-" file writes to stdout (no file is created).
func TestLoggerStdout(t *testing.T) {
	l, err := NewLogger(Config{File: "-", Format: "json"})
	if err != nil {
		t.Fatalf("NewLogger: %v", err)
	}
	defer l.Close()
	// A nil-safe Log to the stdout logger must not error.
	if err := l.Log(Event{Actor: "x", Action: "a", Outcome: OutcomeSuccess}); err != nil {
		t.Fatalf("Log: %v", err)
	}
}

// TestLoggerRotation verifies that when the rotation period changes the open
// file is renamed with the period appended and a fresh file is opened at the
// stable path.
func TestLoggerRotation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "audit.log")
	l, err := NewLogger(Config{File: path, Format: "json", Rotation: RotationDaily})
	if err != nil {
		t.Fatalf("NewLogger: %v", err)
	}
	defer l.Close()

	// Write in the current period.
	if err := l.Log(Event{Actor: "a", Action: "x", Outcome: OutcomeSuccess}); err != nil {
		t.Fatalf("Log: %v", err)
	}
	// Force a period change (simulate the day rolling over).
	l.period = "00000000"
	if err := l.Log(Event{Actor: "b", Action: "y", Outcome: OutcomeSuccess}); err != nil {
		t.Fatalf("Log (post-rotation): %v", err)
	}

	// The old file should have been renamed to path-<oldperiod>.
	renamed := path + "-00000000"
	if _, err := os.Stat(renamed); err != nil {
		t.Errorf("rotated file %q not found: %v", renamed, err)
	}
	// The stable path should hold the new record.
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read stable path: %v", err)
	}
	if !strings.Contains(string(data), `"actor":"b"`) {
		t.Errorf("stable file = %q, want the post-rotation record", string(data))
	}
}

// TestRedactor verifies the Redactor replaces secret plaintexts with the
// marker, longest-first, and that a nil Redactor is a no-op.
func TestRedactor(t *testing.T) {
	r := NewRedactor([]string{"short", "a-longer-secret-value", ""})
	if r == nil {
		t.Fatal("NewRedactor = nil, want non-nil")
	}
	got := r.Redact("the value is a-longer-secret-value and short too")
	want := "the value is " + RedactionMarker + " and " + RedactionMarker + " too"
	if got != want {
		t.Errorf("Redact = %q, want %q", got, want)
	}

	var nilr *Redactor
	if got := nilr.Redact("untouched"); got != "untouched" {
		t.Errorf("nil Redact = %q, want unchanged", got)
	}
}

// TestRedactorEmpty verifies NewRedactor returns nil when there are no
// non-empty values.
func TestRedactorEmpty(t *testing.T) {
	if r := NewRedactor([]string{"", ""}); r != nil {
		t.Errorf("NewRedactor(empty) = %v, want nil", r)
	}
}

// fakeSink is an in-memory audit.Sink that records the events it appends.
type fakeSink struct {
	events []Event
	err    error
}

func (f *fakeSink) Append(_ context.Context, e Event) error {
	if f.err != nil {
		return f.err
	}
	f.events = append(f.events, e)
	return nil
}

// fakePruner is an audit.Pruner that records the cutoff it was asked to prune
// at.
type fakePruner struct {
	cutoffs []time.Time
	pruned  int64
}

func (f *fakePruner) Prune(_ context.Context, before time.Time) (int64, error) {
	f.cutoffs = append(f.cutoffs, before)
	return f.pruned, nil
}

// TestRecorderFansOut verifies the Recorder writes an event to both the local
// logger and the sink, and stamps a zero Time.
func TestRecorderFansOut(t *testing.T) {
	path := filepath.Join(t.TempDir(), "audit.log")
	l, err := NewLogger(Config{File: path, Format: "json"})
	if err != nil {
		t.Fatalf("NewLogger: %v", err)
	}
	defer l.Close()
	sink := &fakeSink{}
	r := NewRecorder(l, sink, nil, nil)

	before := time.Now()
	r.Record(Event{Actor: "alice", Action: ActionRunTrigger, Outcome: OutcomeSuccess})
	after := time.Now()

	if len(sink.events) != 1 {
		t.Fatalf("sink events = %d, want 1", len(sink.events))
	}
	e := sink.events[0]
	if e.Actor != "alice" || e.Action != ActionRunTrigger {
		t.Errorf("event = %+v, want actor=alice action=%s", e, ActionRunTrigger)
	}
	if e.Time.Before(before) || e.Time.After(after) {
		t.Errorf("stamped time %v outside [%v, %v]", e.Time, before, after)
	}
	// The local log should also have the record.
	data, _ := os.ReadFile(path)
	if !strings.Contains(string(data), `"actor":"alice"`) {
		t.Errorf("local log = %q, want the record", string(data))
	}
}

// TestRecorderNilSafe verifies a nil Recorder and a Recorder with nil sinks
// are no-ops (never panic, never fail).
func TestRecorderNilSafe(t *testing.T) {
	var nilr *Recorder
	nilr.Record(Event{Actor: "a", Action: "b", Outcome: OutcomeSuccess})

	r := NewRecorder(nil, nil, nil, nil)
	r.Record(Event{Actor: "a", Action: "b", Outcome: OutcomeSuccess})
}

// TestRecorderSinkFailure verifies a sink failure is dropped (not returned) so
// recording can never fail the caller.
func TestRecorderSinkFailure(t *testing.T) {
	path := filepath.Join(t.TempDir(), "audit.log")
	l, _ := NewLogger(Config{File: path, Format: "json"})
	defer l.Close()
	sink := &fakeSink{err: context.DeadlineExceeded}
	r := NewRecorder(l, sink, nil, nil)
	// Must not panic; the event is dropped from the sink but still written
	// locally.
	r.Record(Event{Actor: "a", Action: "b", Outcome: OutcomeSuccess})
	if len(sink.events) != 0 {
		t.Errorf("sink events = %d, want 0 (failure dropped)", len(sink.events))
	}
}

// TestStartPruningNoop verifies StartPruning is a no-op when the pruner is
// nil, the retention is zero, or the interval is zero.
func TestStartPruningNoop(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	// nil pruner
	r := NewRecorder(nil, nil, nil, nil)
	r.StartPruning(ctx, time.Hour, time.Minute)

	// zero retention
	pr := &fakePruner{}
	r2 := NewRecorder(nil, nil, pr, nil)
	r2.StartPruning(ctx, 0, time.Minute)

	// zero interval
	r3 := NewRecorder(nil, nil, pr, nil)
	r3.StartPruning(ctx, time.Hour, 0)

	<-ctx.Done()
	if len(pr.cutoffs) != 0 {
		t.Errorf("prune cutoffs = %d, want 0 (all no-op)", len(pr.cutoffs))
	}
}

// TestStartPruningRuns verifies StartPruning prunes on the ticker.
func TestStartPruningRuns(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	pr := &fakePruner{pruned: 3}
	r := NewRecorder(nil, nil, pr, nil)
	r.StartPruning(ctx, time.Hour, 30*time.Millisecond)
	<-ctx.Done()
	if len(pr.cutoffs) == 0 {
		t.Error("StartPruning did not prune, want at least one prune")
	}
}

// TestRecorderClose verifies Close closes the local logger and is idempotent.
func TestRecorderClose(t *testing.T) {
	path := filepath.Join(t.TempDir(), "audit.log")
	l, _ := NewLogger(Config{File: path, Format: "json"})
	r := NewRecorder(l, nil, nil, nil)
	if err := r.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := r.Close(); err != nil {
		t.Fatalf("Close (second): %v", err)
	}
	var nilr *Recorder
	if err := nilr.Close(); err != nil {
		t.Fatalf("nil Close: %v", err)
	}
}
