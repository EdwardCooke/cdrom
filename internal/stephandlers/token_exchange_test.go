package stephandlers

import (
	"context"
	"errors"
	"testing"
	"time"

	"cdrom/internal/executor"

	dbpb "cdrom/internal/gen/cdrom/db/v1"
)

// fakeExchanger is a stub executor.TokenExchange for step-handler tests: it
// records the audience and lifetime each exchange requested and returns a
// token derived from the audience (so a test can assert the right audience was
// requested).
type fakeExchanger struct {
	audiences []string
	expiresIn []time.Duration
	token     string // token returned for every exchange; empty derives from audience
}

func (f *fakeExchanger) Exchange(ctx context.Context, audience string, expiresIn time.Duration) (string, error) {
	f.audiences = append(f.audiences, audience)
	f.expiresIn = append(f.expiresIn, expiresIn)
	if f.token != "" {
		return f.token, nil
	}
	return "token-for-" + audience, nil
}

// tokenExchangeStep builds a token_exchange step with the given audience,
// optional expires_in, and optional output name.
func tokenExchangeStep(t *testing.T, audience, expiresIn, output string, outputs []string) *dbpb.JobStep {
	t.Helper()
	params := map[string]*dbpb.ParamValue{}
	if audience != "" {
		params[ParamAudience] = stringParam(audience)
	}
	if expiresIn != "" {
		params[ParamExpiresIn] = stringParam(expiresIn)
	}
	if output != "" {
		params[ParamOutput] = stringParam(output)
	}
	return &dbpb.JobStep{Type: TypeTokenExchange, Params: params, Outputs: outputs}
}

// TestTokenExchangeStepExchangesAndWritesOutput verifies that a token_exchange
// step requests a token for its audience (via the target's TokenExchange) and
// writes the exchanged token into its declared output, so the executor records
// it as the step's output (and the job's aggregated output).
func TestTokenExchangeStepExchangesAndWritesOutput(t *testing.T) {
	exchanger := &fakeExchanger{}
	collector := &executor.StepResultCollector{}
	ctx := executor.ContextWithTokenExchange(context.Background(), exchanger)
	ctx = executor.ContextWithStepStatusReporter(ctx, collector)

	step := tokenExchangeStep(t, "outside-svc", "", "", []string{"token"})
	if err := executor.Execute(ctx, &dbpb.JobSpec{Steps: []*dbpb.JobStep{step}}, testLogger()); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if len(exchanger.audiences) != 1 || exchanger.audiences[0] != "outside-svc" {
		t.Errorf("exchanged for audiences %v, want [outside-svc]", exchanger.audiences)
	}
	results := collector.All()
	if len(results) != 1 {
		t.Fatalf("results = %+v, want one result", results)
	}
	if got := results[0].Outputs["token"]; got != "token-for-outside-svc" {
		t.Errorf("outputs[token] = %q, want %q", got, "token-for-outside-svc")
	}
	if got := collector.JobOutputs()["token"]; got != "token-for-outside-svc" {
		t.Errorf("jobOutputs[token] = %q, want %q", got, "token-for-outside-svc")
	}
}

// TestTokenExchangeStepCustomOutputName verifies that the exchanged token is
// written under the step's "output" name (not the default "token").
func TestTokenExchangeStepCustomOutputName(t *testing.T) {
	exchanger := &fakeExchanger{token: "fixed-token"}
	collector := &executor.StepResultCollector{}
	ctx := executor.ContextWithTokenExchange(context.Background(), exchanger)
	ctx = executor.ContextWithStepStatusReporter(ctx, collector)

	step := tokenExchangeStep(t, "outside-svc", "", "my_token", []string{"my_token"})
	if err := executor.Execute(ctx, &dbpb.JobSpec{Steps: []*dbpb.JobStep{step}}, testLogger()); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	results := collector.All()
	if len(results) != 1 {
		t.Fatalf("results = %+v, want one result", results)
	}
	if got := results[0].Outputs["my_token"]; got != "fixed-token" {
		t.Errorf("outputs[my_token] = %q, want %q", got, "fixed-token")
	}
}

// TestTokenExchangeStepPassesExpiresIn verifies that the step's expires_in
// param (a duration) is parsed and passed to the target's TokenExchange.
func TestTokenExchangeStepPassesExpiresIn(t *testing.T) {
	exchanger := &fakeExchanger{}
	ctx := executor.ContextWithTokenExchange(context.Background(), exchanger)

	step := tokenExchangeStep(t, "outside-svc", "15m", "", nil)
	if err := executor.Execute(ctx, &dbpb.JobSpec{Steps: []*dbpb.JobStep{step}}, testLogger()); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if len(exchanger.expiresIn) != 1 || exchanger.expiresIn[0] != 15*time.Minute {
		t.Errorf("expires_in = %v, want [15m0s]", exchanger.expiresIn)
	}
}

// TestTokenExchangeStepRequiresAudience verifies that a token_exchange step
// with no audience param fails the job.
func TestTokenExchangeStepRequiresAudience(t *testing.T) {
	exchanger := &fakeExchanger{}
	ctx := executor.ContextWithTokenExchange(context.Background(), exchanger)

	step := tokenExchangeStep(t, "", "", "", nil)
	if err := executor.Execute(ctx, &dbpb.JobSpec{Steps: []*dbpb.JobStep{step}}, testLogger()); err == nil {
		t.Fatal("expected an error for a token_exchange step with no audience, got nil")
	}
	if len(exchanger.audiences) != 0 {
		t.Errorf("exchanger was called for a step with no audience (%v)", exchanger.audiences)
	}
}

// TestTokenExchangeStepRequiresExchanger verifies that a token_exchange step
// fails when the execution target provides no TokenExchange (the handler cannot
// mint a token on the target's behalf).
func TestTokenExchangeStepRequiresExchanger(t *testing.T) {
	// No TokenExchange in the context.
	step := tokenExchangeStep(t, "outside-svc", "", "", nil)
	if err := executor.Execute(context.Background(), &dbpb.JobSpec{Steps: []*dbpb.JobStep{step}}, testLogger()); err == nil {
		t.Fatal("expected an error when no TokenExchange is set, got nil")
	}
}

// TestTokenExchangeStepInvalidExpiresIn verifies that a token_exchange step
// whose expires_in is not a valid duration fails the job (a spec error).
func TestTokenExchangeStepInvalidExpiresIn(t *testing.T) {
	exchanger := &fakeExchanger{}
	ctx := executor.ContextWithTokenExchange(context.Background(), exchanger)

	step := tokenExchangeStep(t, "outside-svc", "not-a-duration", "", nil)
	if err := executor.Execute(ctx, &dbpb.JobSpec{Steps: []*dbpb.JobStep{step}}, testLogger()); err == nil {
		t.Fatal("expected an error for an invalid expires_in, got nil")
	}
	if len(exchanger.audiences) != 0 {
		t.Errorf("exchanger was called despite an invalid expires_in (%v)", exchanger.audiences)
	}
}

// TestTokenExchangeStepExchangeError verifies that a failure from the target's
// TokenExchange fails the step (and the job).
func TestTokenExchangeStepExchangeError(t *testing.T) {
	exchanger := &failingExchanger{}
	ctx := executor.ContextWithTokenExchange(context.Background(), exchanger)

	step := tokenExchangeStep(t, "outside-svc", "", "", nil)
	if err := executor.Execute(ctx, &dbpb.JobSpec{Steps: []*dbpb.JobStep{step}}, testLogger()); err == nil {
		t.Fatal("expected an error when the exchange fails, got nil")
	}
}

// failingExchanger is a TokenExchange that always fails, for testing the
// handler's error path.
type failingExchanger struct{}

func (f *failingExchanger) Exchange(ctx context.Context, audience string, expiresIn time.Duration) (string, error) {
	return "", errors.New("exchange failed")
}
