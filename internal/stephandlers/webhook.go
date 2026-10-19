package stephandlers

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"cdrom/internal/executor"

	dbpb "cdrom/internal/gen/cdrom/db/v1"
)

// TypeWebhook is the step type of the built-in webhook notification handler
// (F-18). A step of this type performs an HTTP request from the API to the
// step's URL: the target renders the step's fields against the step's
// condition context and hands the rendered values to the API, which performs
// the request within its configured response timeout. The target never opens an
// outbound HTTP connection itself.
//
// The handler reads its settings from the step's Params map:
//   - "url" (string) — the request URL (required).
//   - "method" (string) — the HTTP method (default "POST").
//   - "headers" (list of "Name: Value" entries) — the request headers; any
//     number of headers, each a template so both the name and the value can
//     reference the condition context. Each entry is rendered and then split on
//     the first ":" into name and value.
//   - "body" (string) — the request body.
//   - "content_type" (string) — how the body is sent: "plain" (sent as-is,
//     e.g. JSON/YAML), "html" (sent as-is), or "markdown" (rendered to HTML by
//     the API). Default "plain".
//   - "timeout" (string, a Go duration) — the response timeout the step
//     requests; the API clamps it to its configured maximum. When empty the API
//     uses its configured default.
//
// The url/headers/body fields are Go templates rendered against the step's
// condition context (its own env, the prior steps' status/outputs, the
// upstream jobs' status/outputs, the job's identity, the run's parameters, and
// the pipeline's named secrets as plaintext) — the same interpolation a step's
// condition (F-06) and the run's spec fields (F-10) use. So a field can
// reference a secret (`{{ .secrets.name }}`), a prior step's output, a run
// parameter, or the job's identity.
//
// The step's own Timeout bounds the request; a job-level timeout (the spec's
// Timeout) also bounds it. A request that fails (the API reports a connection
// error, a non-2xx response, or a timeout) fails the step (and thus the job)
// unless the step sets ignore_failed (F-06).
const TypeWebhook = executor.TypeWebhook

// Param keys the webhook handler reads from a step's Params map.
const (
	ParamURL       = "url"
	ParamMethod    = "method"
	ParamHeaders   = "headers"
	ParamWebhook   = "body"
	ParamWebhookCT = "content_type"
	ParamTimeout   = "timeout"
)

// defaultWebhookContentType is the webhook body's content type when the step
// does not set the "content_type" param.
const defaultWebhookContentType = "plain"

// defaultWebhookMethod is the HTTP method when the step does not set the
// "method" param.
const defaultWebhookMethod = "POST"

func init() {
	executor.RegisterStepType(TypeWebhook, runWebhookStep)
}

// runWebhookStep is the built-in "webhook" step handler (F-18). It renders the
// step's fields against the step's condition context and asks the API to
// perform the HTTP request (the API performs the request within its configured
// response timeout). The target never opens an outbound HTTP connection itself.
func runWebhookStep(ctx context.Context, step *dbpb.JobStep, logger *slog.Logger) error {
	notifier := executor.NotifierFromContext(ctx)
	if notifier == nil {
		return fmt.Errorf("webhook is not supported by this execution target")
	}

	condCtx := executor.ConditionContextFromContext(ctx)
	sink := executor.LogSinkFromContext(ctx)
	stepName := executor.StepNameFromContext(ctx)

	// Render the url against the step's condition context (F-18): the same
	// data a step's condition (F-06) and the run's spec fields (F-10) use.
	url, err := renderField(step, ParamURL, condCtx)
	if err != nil {
		return err
	}
	if url == "" {
		return fmt.Errorf("url is required")
	}

	// Render the headers: each entry is a "Name: Value" template, rendered
	// against the condition context and then split on the first ":" into name
	// and value.
	headers, err := renderHeaders(step, condCtx)
	if err != nil {
		return err
	}

	// Render the body against the condition context.
	body, err := renderField(step, ParamWebhook, condCtx)
	if err != nil {
		return err
	}

	// The content type selects how the body is sent (plain / html / markdown);
	// it is a simple config value, not a template.
	contentType := paramString(step, ParamWebhookCT)
	if contentType == "" {
		contentType = defaultWebhookContentType
	}

	// The method is a simple config value (not a template).
	method := paramString(step, ParamMethod)
	if method == "" {
		method = defaultWebhookMethod
	}

	// The optional per-request timeout (a Go duration); the API clamps it to
	// its configured maximum. When empty the API uses its configured default.
	var timeout time.Duration
	if s := paramString(step, ParamTimeout); s != "" {
		d, err := time.ParseDuration(s)
		if err != nil {
			return fmt.Errorf("timeout %q is not a valid duration: %w", s, err)
		}
		timeout = d
	}

	req := &executor.WebhookRequest{
		URL:         url,
		Method:      method,
		Headers:     headers,
		Body:        body,
		ContentType: contentType,
		Timeout:     timeout,
	}

	if err := notifier.SendWebhook(ctx, req); err != nil {
		return fmt.Errorf("send webhook: %w", err)
	}
	if sink != nil {
		sink.WriteStepOutput(stepName, "stdout", []byte("sent webhook to "+url))
	}
	logger.Info("executor: sent webhook", "url", url, "method", method, "content_type", contentType)
	return nil
}

// renderHeaders renders the step's "headers" param (a list of "Name: Value"
// entries) against the condition context. Each entry is rendered and then split
// on the first ":" into name and value, so both the name and the value can
// reference the condition context. An entry without a ":" is an error. It
// returns the rendered "Name: Value" entries, or nil when the param is absent.
func renderHeaders(step *dbpb.JobStep, condCtx map[string]any) ([]string, error) {
	raw := step.GetParams()[ParamHeaders]
	if raw == nil || len(raw.GetStrings()) == 0 {
		return nil, nil
	}
	out := make([]string, 0, len(raw.GetStrings()))
	for _, entry := range raw.GetStrings() {
		if entry == "" {
			continue
		}
		rendered, err := executor.RenderTemplate(entry, condCtx)
		if err != nil {
			return nil, fmt.Errorf("headers: %w", err)
		}
		name, value, ok := strings.Cut(rendered, ":")
		if !ok {
			return nil, fmt.Errorf("headers: %q is not a \"Name: Value\" entry", rendered)
		}
		name = strings.TrimSpace(name)
		if name == "" {
			return nil, fmt.Errorf("headers: %q has an empty name", rendered)
		}
		out = append(out, name+": "+strings.TrimSpace(value))
	}
	return out, nil
}
