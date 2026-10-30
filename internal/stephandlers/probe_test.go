// Package stephandlers — F-22 probe handler tests.
//
// These tests exercise the built-in "probe" step handler against a LOCAL
// httptest server: success on an accepted status, classified failure on a
// non-accepted status (short deadline), and connection-refusal classification.
package stephandlers

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"

	dbpb "cdrom/internal/gen/cdrom/db/v1"
)

func probeLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(probeDiscard{}, nil))
}

type probeDiscard struct{}

func (probeDiscard) Write(p []byte) (int, error) { return len(p), nil }

// TestProbeAcceptedStatusSucceeds verifies a probe that FINDS an accepted
// status succeeds (F-22).
func TestProbeAcceptedStatusSucceeds(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	step := &dbpb.JobStep{
		Type:   TypeProbe,
		Params: map[string]*dbpb.ParamValue{"url": {String_: srv.URL}},
	}
	err := runProbeStep(context.Background(), step, probeLogger())
	if err != nil {
		t.Fatalf("runProbeStep = %v, want success (endpoint returns 200)", err)
	}
}

// TestProbeUnexpectedStatusFails verifies a probe that sees a NON-ACCEPTED
// status until the deadline FAILS with a classified "unexpected status"
// diagnostic (F-22).
func TestProbeUnexpectedStatusFails(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	step := &dbpb.JobStep{
		Type: TypeProbe,
		Params: map[string]*dbpb.ParamValue{
			"url":      {String_: srv.URL},
			"deadline": {String_: "300ms"},
			"interval": {String_: "50ms"},
		},
	}
	err := runProbeStep(context.Background(), step, probeLogger())
	if err == nil {
		t.Fatal("runProbeStep = nil, want a failure (endpoint returns 503)")
	}
	if !strings.Contains(err.Error(), "unexpected status 503") {
		t.Errorf("error = %q, want a classified 'unexpected status 503'", err.Error())
	}
}

// TestProbeConnectErrorFails verifies a probe that CANNOT CONNECT (closed
// port) FAILS with a classified "connect-error" diagnostic (infrastructure
// trouble, distinguished from application trouble) (F-22).
func TestProbeConnectErrorFails(t *testing.T) {
	// Bind then CLOSE a server to get a definitely-unused localhost port.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	addr := srv.Listener.Addr().String()
	srv.Close()

	step := &dbpb.JobStep{
		Type: TypeProbe,
		Params: map[string]*dbpb.ParamValue{
			"url":      {String_: "http://" + addr + "/healthz"},
			"deadline": {String_: "300ms"},
			"interval": {String_: "50ms"},
		},
	}
	err := runProbeStep(context.Background(), step, probeLogger())
	if err == nil {
		t.Fatal("runProbeStep = nil, want a failure (nothing listening)")
	}
	if !strings.Contains(err.Error(), "connect-error") {
		t.Errorf("error = %q, want a classified 'connect-error'", err.Error())
	}
}

// TestProbeInvalidSchemeRejected verifies a probe with a NON-http(s) URL is a
// spec error (F-22).
func TestProbeInvalidSchemeRejected(t *testing.T) {
	step := &dbpb.JobStep{
		Type:   TypeProbe,
		Params: map[string]*dbpb.ParamValue{"url": {String_: "ftp://nope"}},
	}
	err := runProbeStep(context.Background(), step, probeLogger())
	if err == nil || !strings.Contains(err.Error(), "must be http or https") {
		t.Fatalf("runProbeStep = %v, want a scheme rejection", err)
	}
}

// TestProbeMissingURLError verifies a probe with NO url is a spec error
// (F-22).
func TestProbeMissingURLError(t *testing.T) {
	step := &dbpb.JobStep{Type: TypeProbe}
	err := runProbeStep(context.Background(), step, probeLogger())
	if err == nil || !strings.Contains(err.Error(), "url is required") {
		t.Fatalf("runProbeStep = %v, want 'url is required'", err)
	}
}

// TestProbeCustomExpectVerified verifies a CUSTOM expect set (F-22): an
// endpoint returning 503 is HEALTHY when the step expects it (some systems
// signal readiness with a sentinel status).
func TestProbeCustomExpectVerified(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	step := &dbpb.JobStep{
		Type: TypeProbe,
		Params: map[string]*dbpb.ParamValue{
			"url":    {String_: srv.URL},
			"expect": {String_: "503"},
		},
	}
	if err := runProbeStep(context.Background(), step, probeLogger()); err != nil {
		t.Fatalf("runProbeStep = %v, want success (503 is the expected sentinel)", err)
	}
}

// TestProbeTimeoutClampSanity verifies the probe respects the step's TIMEOUT
// clamp (F-22): a huge deadline is clamped to the step timeout, so the probe
// cannot outlive the step's allotted time. (Asserts the clamp is applied, not
// the full timing.)
func TestProbeTimeoutClampSanity(t *testing.T) {
	step := &dbpb.JobStep{
		Type:    TypeProbe,
		Timeout: durationpb.New(100 * time.Millisecond),
		Params: map[string]*dbpb.ParamValue{
			"url":      {String_: "http://127.0.0.1:1/never"},
			"deadline": {String_: "1h"},
			"interval": {String_: "10ms"},
		},
	}
	start := time.Now()
	err := runProbeStep(context.Background(), step, probeLogger())
	if err == nil {
		t.Fatal("runProbeStep = nil, want a deadline failure (nothing listens)")
	}
	// The clamped deadline (100ms) must dominate, not the 1h param.
	if took := time.Since(start); took > 2*time.Second {
		t.Errorf("probe took %v; the step timeout (100ms) should have clamped the 1h deadline", took)
	}
}
