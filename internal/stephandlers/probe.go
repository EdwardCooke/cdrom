// Package stephandlers — the built-in "probe" step handler (F-22).
//
// A probe step POLLS AN ENDPOINT FROM THE EXECUTION TARGET until it responds
// with an accepted status (verification passed) or a deadline expires
// (verification failed). It is the readiness check that confirms a release is
// healthy after a deploy job.
//
// Executed by the target (stdlib net/http only — portable Windows/Linux),
// DELIBERATELY CONTRASTING the F-18 notification handlers (which act from the
// API): a health probe's value is network ADJACENCY to the workload that was
// just deployed — cluster-internal VIPs/load balancers the API cannot reach.
// Trust parity is the shell handler's (authors control targets and URLs
// alike); secret material arrives via the usual {{ .secrets.name }}
// interpolation and is redacted from logged output (F-12) — the handler logs
// status lines and timings, never response bodies.
//
// The handler reads its settings from the step's Params map. The params are
// in the executor's LAZY-RENDER WHITELIST: they arrive unrendered (raw
// templates) and are rendered HERE against the step's condition context (the
// same data a step's condition uses — prior steps' outputs, run params,
// secrets), so a URL may cite a prior step's output (e.g. a dynamically
// allocated ingress host):
//   - "url" (string) — the probed URL (REQUIRED; http/https).
//   - "method" (string) — the HTTP method. Default "GET".
//   - "expect" (string or list) — the ACCEPTED status codes, space-/comma-
//     separated. Default "200 201 202 204 301 302 303 307 308".
//   - "interval" (duration string) — the poll cadence. Default "10s".
//   - "deadline" (duration string) — the TOTAL WAIT before the step fails.
//     Default "5m". Hard-clamped by the step's own Timeout, when set.
//   - "headers" (list of "Name: Value" lines) — request headers (secrets
//     allowed).
//
// Semantics: poll until a response in `expect` → the step SUCCEEDS; until
// `deadline` exhausts → the step FAILS. The failure message CLASSIFIES the
// last observation — "connect-error ..." (DNS/refused/unreachable:
// infrastructure) vs. "unexpected status <code>" (application answering but
// unhealthy) — so operators triage without opening logs.
//
// Step outputs (F-06) on success: "final_status" (the accepted code), "polls"
// (attempts made), and "elapsed_s" (seconds spent probing).
package stephandlers

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"cdrom/internal/executor"

	dbpb "cdrom/internal/gen/cdrom/db/v1"
)

// TypeProbe is the step type of the built-in probe handler (F-22).
const TypeProbe = executor.TypeProbe

// Param keys the probe handler reads from a step's Params map.
const (
	ParamProbeURL      = "url"
	ParamProbeMethod   = "method"
	ParamProbeExpect   = "expect"
	ParamProbeInterval = "interval"
	ParamProbeDeadline = "deadline"
	ParamProbeHeaders  = "headers"
)

// Probe defaults.
const (
	defaultProbeMethod   = "GET"
	defaultProbeExpect   = "200 201 202 204 301 302 303 307 308"
	defaultProbeInterval = 10 * time.Second
	defaultProbeDeadline = 5 * time.Minute
	maxProbeDrain        = 512 // bytes of response body drained/discarded per probe
	maxProbeRedirects    = 10
)

func init() {
	executor.RegisterStepType(TypeProbe, runProbeStep)
}

// probeSettings is the parsed probe step configuration.
type probeSettings struct {
	url      string
	method   string
	expect   map[int]bool
	interval time.Duration
	deadline time.Duration
	headers  [][2]string
}

// parseProbeSettings extracts and validates the probe step's params,
// rendering each against the step's condition context (lazy rendering, F-22).
func parseProbeSettings(condCtx map[string]any, step *dbpb.JobStep) (*probeSettings, error) {
	settings := &probeSettings{
		method:   defaultProbeMethod,
		interval: defaultProbeInterval,
		deadline: defaultProbeDeadline,
	}
	rawURL, err := renderProbeScalar(condCtx, step, ParamProbeURL)
	if err != nil {
		return nil, err
	}
	if rawURL == "" {
		return nil, fmt.Errorf("probe: url is required")
	}
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("probe: url %q: %w", rawURL, err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return nil, fmt.Errorf("probe: url %q must be http or https", rawURL)
	}
	settings.url = rawURL
	if m, err := renderProbeScalar(condCtx, step, ParamProbeMethod); err == nil && m != "" {
		settings.method = strings.ToUpper(m)
	}
	expRaw, _ := renderProbeScalar(condCtx, step, ParamProbeExpect)
	if expRaw == "" {
		expRaw = defaultProbeExpect
	}
	set, err := parseExpectCodes(expRaw)
	if err != nil {
		return nil, err
	}
	settings.expect = set
	if iv, err := renderProbeScalar(condCtx, step, ParamProbeInterval); err == nil && iv != "" {
		d, err := time.ParseDuration(iv)
		if err != nil {
			return nil, fmt.Errorf("probe: interval %q: %w", iv, err)
		}
		if d <= 0 {
			return nil, fmt.Errorf("probe: interval must be positive")
		}
		settings.interval = d
	}
	if dl, err := renderProbeScalar(condCtx, step, ParamProbeDeadline); err == nil && dl != "" {
		d, err := time.ParseDuration(dl)
		if err != nil {
			return nil, fmt.Errorf("probe: deadline %q: %w", dl, err)
		}
		if d <= 0 {
			return nil, fmt.Errorf("probe: deadline must be positive")
		}
		settings.deadline = d
	}
	// Clamp the deadline by the step's own timeout, when set (the executor
	// enforces the step timeout on the handler's context; clamping keeps the
	// diagnostic accurate).
	if stepTimeout := step.GetTimeout().AsDuration(); stepTimeout > 0 && settings.deadline > stepTimeout {
		settings.deadline = stepTimeout
	}
	headerVals, _ := renderProbeList(condCtx, step, ParamProbeHeaders)
	for _, line := range headerVals {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		idx := strings.Index(line, ":")
		if idx <= 0 {
			return nil, fmt.Errorf("probe: header %q must be of the form \"Name: Value\"", line)
		}
		settings.headers = append(settings.headers, [2]string{
			strings.TrimSpace(line[:idx]), strings.TrimSpace(line[idx+1:])})
	}
	return settings, nil
}

// parseExpectCodes parses a space-, comma-, or newline-separated list of HTTP
// status codes.
func parseExpectCodes(raw string) (map[int]bool, error) {
	fields := strings.FieldsFunc(raw, func(r rune) bool {
		return r == ',' || r == '\n' || r == '\t' || r == ' '
	})
	set := make(map[int]bool, len(fields))
	for _, f := range fields {
		code, err := strconv.Atoi(f)
		if err != nil {
			return nil, fmt.Errorf("probe: expect %q is not a status code", f)
		}
		if code < 100 || code > 599 {
			return nil, fmt.Errorf("probe: expect %d is not a valid status code", code)
		}
		set[code] = true
	}
	if len(set) == 0 {
		return nil, fmt.Errorf("probe: expect must name at least one status code")
	}
	return set, nil
}

// runProbeStep is the built-in "probe" step handler (F-22). It polls the
// configured URL from the execution target until the endpoint responds with
// an accepted status (success) or the deadline expires (classified failure).
func runProbeStep(ctx context.Context, step *dbpb.JobStep, logger *slog.Logger) error {
	condCtx := executor.ConditionContextFromContext(ctx)
	settings, err := parseProbeSettings(condCtx, step)
	if err != nil {
		return err
	}
	client := newProbeClient()
	defer client.CloseIdleConnections()

	start := time.Now()
	remaining := settings.deadline
	lastClassified := "connect-error"
	lastDetail := ""
	polls := 0

	for {
		polls++
		classified, detail := probeOnce(ctx, client, settings)
		lastClassified, lastDetail = classified, detail
		logger.Debug("executor: probe poll", "step", probeStepName(step), "url", maskURL(settings.url), "class", classified, "detail", detail, "poll", polls)
		if classified == "accepted" {
			_ = writeStepOutput(step, "final_status", detail)
			_ = writeStepOutput(step, "polls", strconv.Itoa(polls))
			_ = writeStepOutput(step, "elapsed_s", strconv.FormatFloat(time.Since(start).Seconds(), 'f', 1, 64))
			logger.Info("executor: probe succeeded", "step", probeStepName(step), "url", maskURL(settings.url), "status", detail, "polls", polls, "elapsed", time.Since(start).Round(time.Millisecond))
			return nil
		}
		wait := settings.interval
		if wait > remaining {
			wait = remaining
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			// The step (or job) was cancelled/timed out by the executor.
			return ctx.Err()
		case <-timer.C:
			remaining -= wait
			if remaining <= 0 {
				if lastClassified == "connect-error" {
					return fmt.Errorf("probe: deadline (%s) reached; last observation: connect-error (%s)", settings.deadline, lastDetail)
				}
				return fmt.Errorf("probe: deadline (%s) reached; last observation: unexpected status %s", settings.deadline, lastDetail)
			}
		}
	}
}

// probeOnce performs a single probe attempt. Classification:
//   - "accepted" — the response status is in the expected set (detail = code);
//   - "connect-error" — DNS/TCP/TLS/connection failure (detail = the error);
//   - "unexpected" — connected, but the status is not accepted (detail = code).
func probeOnce(ctx context.Context, client *http.Client, settings *probeSettings) (classification, detail string) {
	req, err := http.NewRequestWithContext(ctx, settings.method, settings.url, nil)
	if err != nil {
		return "connect-error", err.Error()
	}
	for _, hv := range settings.headers {
		req.Header.Add(hv[0], hv[1])
	}
	resp, err := client.Do(req)
	if err != nil {
		return "connect-error", err.Error()
	}
	defer resp.Body.Close()
	// Drain a bounded amount of the body (DISCARDED — response bodies are
	// never logged) so the connection can be reused and the server is not
	// left hanging.
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxProbeDrain))
	code := strconv.Itoa(resp.StatusCode)
	if settings.expect[resp.StatusCode] {
		return "accepted", code
	}
	return "unexpected", code
}

// newProbeClient builds the probe's HTTP client: redirects are FOLLOWED (a
// readiness endpoint commonly 302s to a portal), bounded to maxProbeRedirects
// hops. System certificate pools are used (no custom CA).
func newProbeClient() *http.Client {
	return &http.Client{
		CheckRedirect: func(_ *http.Request, via []*http.Request) error {
			if len(via) >= maxProbeRedirects {
				return fmt.Errorf("too many redirects")
			}
			return nil
		},
	}
}

// maskURL strips userinfo (basic-auth credentials) from a URL for LOGGING:
// the probe's diagnostics must not leak secrets (F-12 redaction principle).
func maskURL(u string) string {
	parsed, err := url.Parse(u)
	if err != nil {
		return u
	}
	if parsed.User != nil {
		parsed.User = nil
	}
	return parsed.String()
}

// probeStepName returns the step's name (falling back to its type) for logs.
func probeStepName(step *dbpb.JobStep) string {
	if n := step.GetName(); n != "" {
		return n
	}
	return step.GetType()
}

// renderProbeScalar renders the STRING param named key against the step's
// condition context (lazy rendering, F-22). Absent/empty yields ""; a render
// failure is propagated as a spec error.
func renderProbeScalar(condCtx map[string]any, step *dbpb.JobStep, key string) (string, error) {
	raw, ok := step.GetParams()[key]
	if !ok || raw == nil {
		return "", nil
	}
	val := raw.GetString_()
	if val == "" {
		return "", nil
	}
	rendered, err := executor.RenderTemplate(val, condCtx)
	if err != nil {
		return "", fmt.Errorf("probe: %s: %w", key, err)
	}
	return strings.TrimSpace(rendered), nil
}

// renderProbeList renders the LIST param named key against the step's
// condition context.
func renderProbeList(condCtx map[string]any, step *dbpb.JobStep, key string) ([]string, error) {
	raw, ok := step.GetParams()[key]
	if !ok || raw == nil {
		return nil, nil
	}
	var vals []string
	if raw.GetString_() != "" {
		vals = []string{raw.GetString_()}
	} else {
		vals = raw.GetStrings()
	}
	if len(vals) == 0 {
		return nil, nil
	}
	out := make([]string, 0, len(vals))
	for _, v := range vals {
		rendered, err := executor.RenderTemplate(v, condCtx)
		if err != nil {
			return nil, fmt.Errorf("probe: %s: %w", key, err)
		}
		out = append(out, strings.TrimSpace(rendered))
	}
	return out, nil
}
