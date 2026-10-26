package pipelinefile

import (
	"strings"
	"testing"
)

// ---------------------------------------------------------------------------
// F-21.5.1 — spec.periodicRefresh: parse + validate (actionable errors).
// ---------------------------------------------------------------------------

// TestCompilePeriodicRefreshAbsent verifies an ABSENT policy compiles to nil
// (the pipeline inherits the system default cadence).
func TestCompilePeriodicRefreshAbsent(t *testing.T) {
	doc, err := Parse("p.yaml", []byte(strings.ReplaceAll(minimalPipeline, "\n", "\n")))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	pol, errs := CompilePeriodicRefresh("p.yaml", doc.Spec)
	if len(errs) > 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}
	if pol != nil {
		t.Errorf("pol = %+v, want nil (absent policy)", pol)
	}
}

// TestCompilePeriodicRefreshInterval verifies a lone interval (a duration)
// compiles.
func TestCompilePeriodicRefreshInterval(t *testing.T) {
	yaml := minimalPipeline + "  periodicRefresh:\n    interval: 5m\n"
	doc, err := Parse("p.yaml", []byte(yaml))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	pol, errs := CompilePeriodicRefresh("p.yaml", doc.Spec)
	if len(errs) > 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}
	if pol == nil || pol.Interval != "5m" || pol.Schedule != "" {
		t.Errorf("pol = %+v, want {Interval:5m}", pol)
	}
}

// TestCompilePeriodicRefreshCron verifies a lone schedule (a cron expression)
// compiles.
func TestCompilePeriodicRefreshCron(t *testing.T) {
	yaml := minimalPipeline + "  periodicRefresh:\n    schedule: \"0 3 * * *\"\n"
	doc, err := Parse("p.yaml", []byte(yaml))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	pol, errs := CompilePeriodicRefresh("p.yaml", doc.Spec)
	if len(errs) > 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}
	if pol == nil || pol.Schedule != "0 3 * * *" || pol.Interval != "" {
		t.Errorf("pol = %+v, want {Schedule:\"0 3 * * *\"}", pol)
	}
}

// TestCompilePeriodicRefreshBoth verifies BOTH controls compile (the cron
// drives at resolution time — the compiler merely carries both).
func TestCompilePeriodicRefreshBoth(t *testing.T) {
	yaml := minimalPipeline + "  periodicRefresh:\n    interval: 5m\n    schedule: \"0 3 * * *\"\n"
	doc, err := Parse("p.yaml", []byte(yaml))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	pol, errs := CompilePeriodicRefresh("p.yaml", doc.Spec)
	if len(errs) > 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}
	if pol == nil || pol.Interval != "5m" || pol.Schedule != "0 3 * * *" {
		t.Errorf("pol = %+v, want {Interval:5m Schedule:\"0 3 * * *\"}", pol)
	}
}

// TestCompilePeriodicRefreshInvalidDuration verifies a MALFORMED duration is
// an actionable error naming the key.
func TestCompilePeriodicRefreshInvalidDuration(t *testing.T) {
	yaml := minimalPipeline + "  periodicRefresh:\n    interval: banana\n"
	doc, err := Parse("p.yaml", []byte(yaml))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	pol, errs := CompilePeriodicRefresh("p.yaml", doc.Spec)
	if len(errs) == 0 {
		t.Fatalf("want a validation error for a bad duration, got pol=%+v", pol)
	}
	msg := errs.Error()
	if !strings.Contains(msg, "periodicRefresh.interval") {
		t.Errorf("error does not name the key: %q", msg)
	}
	if !strings.Contains(msg, "banana") {
		t.Errorf("error does not cite the offender: %q", msg)
	}
}

// TestCompilePeriodicRefreshInvalidCron verifies a MALFORMED cron is an
// actionable error naming the key.
func TestCompilePeriodicRefreshInvalidCron(t *testing.T) {
	yaml := minimalPipeline + "  periodicRefresh:\n    schedule: \"not a cron\"\n"
	doc, err := Parse("p.yaml", []byte(yaml))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	pol, errs := CompilePeriodicRefresh("p.yaml", doc.Spec)
	if len(errs) == 0 {
		t.Fatalf("want a validation error for a bad cron, got pol=%+v", pol)
	}
	msg := errs.Error()
	if !strings.Contains(msg, "periodicRefresh.schedule") {
		t.Errorf("error does not name the key: %q", msg)
	}
}

// TestCompilePeriodicRefreshUnknownKey verifies an UNKNOWN key is rejected
// (typo protection, like the other spec fields).
func TestCompilePeriodicRefreshUnknownKey(t *testing.T) {
	yaml := minimalPipeline + "  periodicRefresh:\n    frequencey: 5m\n"
	doc, err := Parse("p.yaml", []byte(yaml))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	_, errs := CompilePeriodicRefresh("p.yaml", doc.Spec)
	if len(errs) == 0 {
		t.Fatal("want a validation error for an unknown key")
	}
	if !strings.Contains(errs.Error(), "frequencey") {
		t.Errorf("error does not cite the unknown key: %q", errs.Error())
	}
}

// TestCompilePeriodicRefreshWrongType verifies a NON-MAPPING policy is
// rejected.
func TestCompilePeriodicRefreshWrongType(t *testing.T) {
	yaml := minimalPipeline + "  periodicRefresh: 5m\n"
	doc, err := Parse("p.yaml", []byte(yaml))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	_, errs := CompilePeriodicRefresh("p.yaml", doc.Spec)
	if len(errs) == 0 {
		t.Fatal("want a validation error for a non-mapping policy")
	}
}

// TestCompilePeriodicRefreshEmptyMapping verifies a WHOLLY EMPTY policy
// mapping compiles to nil (equivalent to absent — the system default
// applies).
func TestCompilePeriodicRefreshEmptyMapping(t *testing.T) {
	yaml := minimalPipeline + "  periodicRefresh: {}\n"
	doc, err := Parse("p.yaml", []byte(yaml))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	pol, errs := CompilePeriodicRefresh("p.yaml", doc.Spec)
	if len(errs) > 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}
	if pol != nil {
		t.Errorf("pol = %+v, want nil (wholly empty)", pol)
	}
}
