package stephandlers

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"cdrom/internal/executor"

	dbpb "cdrom/internal/gen/cdrom/db/v1"
)

// TypeEmail is the step type of the built-in email notification handler
// (F-18). A step of this type sends an SMTP message from the API: the target
// renders the step's fields against the step's condition context and hands the
// rendered values to the API, which sends the message from its configured SMTP
// server. The target never opens an SMTP connection itself.
//
// The handler reads its settings from the step's Params map:
//   - "to" (list of addresses, or a single address) — the recipients (required).
//   - "cc" (list of addresses) — the carbon-copy recipients (optional).
//   - "bcc" (list of addresses) — the blind-carbon-copy recipients (optional).
//   - "subject" (string) — the message subject (optional).
//   - "body" (string) — the message body.
//   - "content_type" (string) — how the body is sent: "plain" (a text/plain
//     message), "html" (a text/html message, sent as-is), or "markdown"
//     (rendered to HTML by the API). Default "markdown".
//   - "from" (string) — overrides the API's configured sender for this message.
//   - "reply_to" (string) — sets the message's Reply-To header.
//
// The to/cc/bcc/subject/body fields are Go templates rendered against the
// step's condition context (its own env, the prior steps' status/outputs, the
// upstream jobs' status/outputs, the job's identity, the run's parameters, and
// the pipeline's named secrets as plaintext) — the same interpolation a step's
// condition (F-06) and the run's spec fields (F-10) use. So a field can
// reference a secret (`{{ .secrets.name }}`), a prior step's output, a run
// parameter, or the job's identity.
//
// The step's own Timeout bounds the send; a job-level timeout (the spec's
// Timeout) also bounds it. A send that fails (the API reports an SMTP error)
// fails the step (and thus the job) unless the step sets ignore_failed (F-06).
const TypeEmail = executor.TypeEmail

// Param keys the email handler reads from a step's Params map.
const (
	ParamTo      = "to"
	ParamCC      = "cc"
	ParamBCC     = "bcc"
	ParamSubject = "subject"
	ParamBody    = "body"
	ParamContent = "content_type"
	ParamFrom    = "from"
	ParamReplyTo = "reply_to"
)

// defaultEmailContentType is the email body's content type when the step does
// not set the "content_type" param.
const defaultEmailContentType = "markdown"

func init() {
	executor.RegisterStepType(TypeEmail, runEmailStep)
}

// runEmailStep is the built-in "email" step handler (F-18). It renders the
// step's fields against the step's condition context and asks the API to send
// the SMTP message (the API supplies the sender and the SMTP server). The
// target never opens an SMTP connection itself.
func runEmailStep(ctx context.Context, step *dbpb.JobStep, logger *slog.Logger) error {
	notifier := executor.NotifierFromContext(ctx)
	if notifier == nil {
		return fmt.Errorf("email is not supported by this execution target")
	}

	condCtx := executor.ConditionContextFromContext(ctx)
	sink := executor.LogSinkFromContext(ctx)
	stepName := executor.StepNameFromContext(ctx)

	// Render the recipient fields against the step's condition context (F-18):
	// the same data a step's condition (F-06) and the run's spec fields (F-10)
	// use. A field can reference a secret, a prior step's output, a run
	// parameter, or the job's identity.
	to, err := renderAddresses(step, ParamTo, condCtx)
	if err != nil {
		return err
	}
	if len(to) == 0 {
		return fmt.Errorf("to is required")
	}
	cc, err := renderAddresses(step, ParamCC, condCtx)
	if err != nil {
		return err
	}
	bcc, err := renderAddresses(step, ParamBCC, condCtx)
	if err != nil {
		return err
	}
	subject, err := renderField(step, ParamSubject, condCtx)
	if err != nil {
		return err
	}
	body, err := renderField(step, ParamBody, condCtx)
	if err != nil {
		return err
	}

	// The content type selects how the body is sent (plain / html / markdown);
	// it is a simple config value, not a template.
	contentType := paramString(step, ParamContent)
	if contentType == "" {
		contentType = defaultEmailContentType
	}

	req := &executor.EmailRequest{
		To:          to,
		CC:          cc,
		BCC:         bcc,
		Subject:     subject,
		Body:        body,
		ContentType: contentType,
		From:        paramString(step, ParamFrom),
		ReplyTo:     paramString(step, ParamReplyTo),
	}

	if err := notifier.SendEmail(ctx, req); err != nil {
		return fmt.Errorf("send email: %w", err)
	}
	if sink != nil {
		sink.WriteStepOutput(stepName, "stdout", []byte("sent email to "+strings.Join(to, ", ")))
		sink.WriteStepOutput(stepName, "stdout", []byte("email body:\n"+body))
	}
	logger.Info("executor: sent email", "to", to, "content_type", contentType)
	return nil
}

// renderAddresses renders the recipient field named key (a list of addresses,
// or a single address) against the condition context. It returns the rendered
// addresses, or nil when the field is absent. A field that fails to render is
// an error (a spec error that fails the step).
func renderAddresses(step *dbpb.JobStep, key string, condCtx map[string]any) ([]string, error) {
	raw := step.GetParams()[key]
	if raw == nil {
		return nil, nil
	}
	if len(raw.GetStrings()) > 0 {
		out := make([]string, 0, len(raw.GetStrings()))
		for _, s := range raw.GetStrings() {
			if s == "" {
				continue
			}
			rendered, err := executor.RenderTemplate(s, condCtx)
			if err != nil {
				return nil, fmt.Errorf("%s: %w", key, err)
			}
			out = append(out, rendered)
		}
		return out, nil
	}
	if raw.GetString_() != "" {
		rendered, err := executor.RenderTemplate(raw.GetString_(), condCtx)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", key, err)
		}
		return []string{rendered}, nil
	}
	return nil, nil
}

// renderField renders the scalar field named key against the condition
// context. It returns "" when the field is absent or empty. A field that fails
// to render is an error (a spec error that fails the step).
func renderField(step *dbpb.JobStep, key string, condCtx map[string]any) (string, error) {
	raw := paramString(step, key)
	if raw == "" {
		return "", nil
	}
	rendered, err := executor.RenderTemplate(raw, condCtx)
	if err != nil {
		return "", fmt.Errorf("%s: %w", key, err)
	}
	return rendered, nil
}
