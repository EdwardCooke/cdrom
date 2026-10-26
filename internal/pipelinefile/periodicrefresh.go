package pipelinefile

import (
	"fmt"
	"time"

	"github.com/robfig/cron/v3"
)

// PeriodicRefresh is a pipeline's file-declared periodic-refresh policy
// (F-21.5.1, spec.periodicRefresh): how often an externally-sourced pipeline
// is pulled from its source. It is a behavioral preference of the DEFINITION
// (owned by the pipeline's authors, living in the file) — unlike the source
// ITSELF (where the file is pulled from), which is a database concept
// configured through the API.
//
// Two orthogonal controls:
//
//   - Interval is a steady-state cadence (a duration, e.g. "2m"): the source
//     is due when now >= last_attempt + interval. Everyday freshness.
//   - Schedule is a cron expression (e.g. "0 3 * * *"): PRECISE TIMING — e.g.
//     syncing only during after-hours when load is minimal. When both are set
//     the cron DRIVES and the interval is ignored.
//
// Neither is mandatory: a pipeline that declares no periodicRefresh is
// refreshed on the system default cadence (configured on the API). Whether a
// pipeline's declared policy is honored at all is governed by the admin gate
// (a global config knob + a per-pipeline database flag, F-21.5.1) — the
// compiler merely validates and carries the values.
type PeriodicRefresh struct {
	// Interval is the steady-state refresh cadence (a duration string, e.g.
	// "2m"); empty means unpinned. An explicit "0" opts the source out of
	// periodic polling (honored only when the admin gate allows it).
	Interval string
	// Schedule is a cron expression (five fields: minute, hour, day-of-month,
	// month, day-of-week — the same grammar as a pipeline's cron trigger,
	// F-09); empty means no cron. When non-empty it drives the periodic
	// refresh and the interval is ignored.
	Schedule string
}

// HasAny reports whether the policy declares anything (either control).
func (p *PeriodicRefresh) HasAny() bool {
	return p != nil && (p.Interval != "" || p.Schedule != "")
}

// CompilePeriodicRefresh parses and validates the file's
// spec.periodicRefresh (F-21.5.1). The value is a mapping with two OPTIONAL
// keys: interval (a duration string) and schedule (a cron expression).
// Unknown keys are a validation error (typo protection, like the other spec
// fields). Validation is ACTIONABLE: a malformed duration or cron names the
// offending key and the problem, so the author can fix the file. A policy
// that is absent or wholly empty yields (nil, nil) — the pipeline simply
// inherits the system default cadence.
func CompilePeriodicRefresh(file string, spec Value) (*PeriodicRefresh, Errors) {
	val, ok := spec.Get("periodicRefresh")
	if !ok {
		return nil, nil
	}
	if val.Type != Map {
		return nil, Errors{{File: file, Key: "spec.periodicRefresh",
			Reason: "expected a mapping with optional interval and schedule keys"}}
	}
	base := NewPath("spec", "periodicRefresh")
	var errs Errors
	p := &PeriodicRefresh{}
	for _, e := range val.Entries {
		switch e.Key {
		case "interval":
			if e.Value.Type != Scalar {
				errs = append(errs, Error{File: file, Key: base.Extend("interval").String(),
					Reason: "expected a duration string (e.g. \"2m\")"})
				continue
			}
			v := e.Value.String
			if _, err := time.ParseDuration(v); err != nil {
				errs = append(errs, Error{File: file, Key: base.Extend("interval").String(),
					Reason: fmt.Sprintf("invalid duration %q (%v)", v, err)})
				continue
			}
			p.Interval = v
		case "schedule":
			if e.Value.Type != Scalar {
				errs = append(errs, Error{File: file, Key: base.Extend("schedule").String(),
					Reason: "expected a cron expression (e.g. \"0 3 * * *\")"})
				continue
			}
			v := e.Value.String
			if _, err := cron.ParseStandard(v); err != nil {
				errs = append(errs, Error{File: file, Key: base.Extend("schedule").String(),
					Reason: fmt.Sprintf("invalid cron expression %q (%v)", v, err)})
				continue
			}
			p.Schedule = v
		default:
			errs = append(errs, Error{File: file, Key: base.Extend(e.Key).String(),
				Reason: fmt.Sprintf("unknown key %q (supported: interval, schedule)", e.Key)})
		}
	}
	if len(errs) > 0 {
		return nil, errs
	}
	if !p.HasAny() {
		return nil, nil
	}
	return p, nil
}
