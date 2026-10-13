package notify

// Package notify provides the API-backed implementations of the executor's
// Notifier and OnceClaimer interfaces (F-18, F-01).
//
// A notification step handler (the built-in "email" and "webhook" handlers)
// renders the step's fields against the step's condition context and hands the
// rendered values to the API, which performs the actual send (SMTP for email,
// HTTP for webhook) using its own configuration. The execution target never
// opens an SMTP or outbound HTTP connection itself: it only renders templates
// and calls the API's SendEmail / SendWebhook RPCs, presenting the job's own
// token.
//
// The OnceClaimer enforces a step's `once` field (F-01): a step with `once`
// set runs on at most one target of a job that fans out to a worker group. The
// target calls the API's ClaimStepOnce RPC (which claims a durable slot in the
// Database service, keyed by job id + step index + attempt); when it returns
// false the step was already claimed by another target in the same attempt, so
// the executor skips the step.
//
// Both the long-lived worker and the ephemeral agent use these: each sets them
// in the executor's context (via executor.ContextWithNotifier and
// executor.ContextWithOnceClaimer) before running a job's spec. They are the
// single shared implementations, so the worker and agent do not each carry a
// copy.

import (
	"context"

	"google.golang.org/protobuf/types/known/durationpb"

	"cdrom/internal/executor"
	apipb "cdrom/internal/gen/cdrom/api/v1"
	"cdrom/internal/grpcutil"
)

// Notifier is the API-backed implementation of executor.Notifier. It calls the
// API's SendEmail / SendWebhook RPCs, presenting the job's own token. The API
// performs the actual send (SMTP for email, HTTP for webhook) using its own
// configuration; the target never opens an SMTP or outbound HTTP connection
// itself.
type Notifier struct {
	api   apipb.APIClient
	jobID int64
	token string
}

// NewNotifier builds a Notifier for job, presenting token on its API calls.
func NewNotifier(api apipb.APIClient, jobID int64, token string) *Notifier {
	return &Notifier{api: api, jobID: jobID, token: token}
}

// SendEmail implements executor.Notifier: it asks the API to send an SMTP
// message (F-18), presenting the job's own token. The API supplies the sender
// and the SMTP server; the fields are the step's rendered values.
func (n *Notifier) SendEmail(ctx context.Context, email *executor.EmailRequest) error {
	if email == nil {
		return nil
	}
	_, err := n.api.SendEmail(grpcutil.WithBearerToken(ctx, n.token), &apipb.SendEmailRequest{
		JobId:       n.jobID,
		To:          email.To,
		Cc:          email.CC,
		Bcc:         email.BCC,
		Subject:     email.Subject,
		Body:        email.Body,
		ContentType: email.ContentType,
		From:        email.From,
		ReplyTo:     email.ReplyTo,
	})
	return err
}

// SendWebhook implements executor.Notifier: it asks the API to perform an HTTP
// request to the step's URL (F-18), presenting the job's own token. The API
// performs the request within its configured response timeout.
func (n *Notifier) SendWebhook(ctx context.Context, webhook *executor.WebhookRequest) error {
	if webhook == nil {
		return nil
	}
	_, err := n.api.SendWebhook(grpcutil.WithBearerToken(ctx, n.token), &apipb.SendWebhookRequest{
		JobId:       n.jobID,
		Url:         webhook.URL,
		Method:      webhook.Method,
		Headers:     webhook.Headers,
		Body:        webhook.Body,
		ContentType: webhook.ContentType,
		Timeout:     durationpb.New(webhook.Timeout),
	})
	return err
}

// Claimer is the API-backed implementation of executor.OnceClaimer. It calls
// the API's ClaimStepOnce RPC (which claims a durable slot in the Database
// service, keyed by job id + step index + attempt), presenting the job's own
// token. When it returns false the step was already claimed by another target
// in the same attempt, so the executor skips the step.
type Claimer struct {
	api     apipb.APIClient
	jobID   int64
	token   string
	attempt int32
}

// NewClaimer builds a Claimer for job (attempt), presenting token on its API
// calls.
func NewClaimer(api apipb.APIClient, jobID int64, token string, attempt int32) *Claimer {
	return &Claimer{api: api, jobID: jobID, token: token, attempt: attempt}
}

// Claim implements executor.OnceClaimer: it asks the API to claim the "run
// this step" slot for stepIndex of the job (F-01), presenting the job's own
// token. It returns true when this target won the claim (it should run the
// step) and false when another target already claimed it in the same attempt
// (this target should skip the step).
func (c *Claimer) Claim(ctx context.Context, stepIndex int) (bool, error) {
	resp, err := c.api.ClaimStepOnce(grpcutil.WithBearerToken(ctx, c.token), &apipb.ClaimStepOnceRequest{
		JobId:     c.jobID,
		StepIndex: int32(stepIndex),
		Attempt:   c.attempt,
	})
	if err != nil {
		return false, err
	}
	return resp.GetClaimed(), nil
}

// Ensure the implementations satisfy the executor interfaces.
var (
	_ executor.Notifier    = (*Notifier)(nil)
	_ executor.OnceClaimer = (*Claimer)(nil)
)
