package stephandlers

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"cdrom/internal/executor"

	dbpb "cdrom/internal/gen/cdrom/db/v1"
)

// TypeTokenExchange is the step type of the built-in token-exchange handler.
// A step of this type requests a new job token for a different audience (e.g.
// an outside resource the job needs to call) while the job is running, and
// hands the exchanged token to later steps and jobs as a step output.
//
// The handler reads its settings from the step's Params map:
//   - "audience" (string) — the audience the new token should be valid for
//     (required).
//   - "expires_in" (string) — how long the exchanged token should be valid
//     (a Go duration, e.g. "15m"); empty means the API's default
//     exchanged-token lifetime.
//   - "output" (string) — the name of the step output the exchanged token is
//     written under (default "token").
//
// The handler obtains the exchanged token from the execution target through
// the executor's TokenExchange (set in the context by the worker or agent,
// which calls the API's ExchangeJobToken RPC). The executor gives every step a
// per-step output directory (carried in the step's env as
// executor.StepOutputDirEnv), so the handler always writes the token into it
// under the output name. The executor reads every file in that directory back
// as a step output (file name = output name), which is how the token is
// handed to downstream steps and jobs through the condition context.
const TypeTokenExchange = "token_exchange"

// Param keys the token-exchange handler reads from a step's Params map.
const (
	ParamAudience  = "audience"
	ParamExpiresIn = "expires_in"
	ParamOutput    = "output"
)

// defaultTokenOutput is the step-output name the exchanged token is written
// under when the step does not set the "output" param.
const defaultTokenOutput = "token"

func init() {
	executor.RegisterStepType(TypeTokenExchange, runTokenExchangeStep)
}

// runTokenExchangeStep is the built-in "token_exchange" step handler. It
// requests a new job token for a different audience (via the execution
// target's TokenExchange) and writes the exchanged token into the step's
// output directory (which the executor always creates), so later steps and
// jobs can read it through the condition context.
func runTokenExchangeStep(ctx context.Context, step *dbpb.JobStep, logger *slog.Logger) error {
	audience := paramString(step, ParamAudience)
	if audience == "" {
		return fmt.Errorf("audience is required")
	}

	exchanger := executor.TokenExchangeFromContext(ctx)
	if exchanger == nil {
		return fmt.Errorf("token exchange is not supported by this execution target")
	}

	var expiresIn time.Duration
	if s := paramString(step, ParamExpiresIn); s != "" {
		d, err := time.ParseDuration(s)
		if err != nil {
			return fmt.Errorf("expires_in %q is not a valid duration: %w", s, err)
		}
		expiresIn = d
	}

	token, err := exchanger.Exchange(ctx, audience, expiresIn)
	if err != nil {
		return fmt.Errorf("exchange token for audience %q: %w", audience, err)
	}
	logger.Info("executor: exchanged job token", "audience", audience, "expires_in", expiresIn)

	if dir := step.GetEnv()[executor.StepOutputDirEnv]; dir != "" {
		name := paramString(step, ParamOutput)
		if name == "" {
			name = defaultTokenOutput
		}
		if err := os.WriteFile(filepath.Join(dir, name), []byte(token), 0o600); err != nil {
			return fmt.Errorf("write exchanged token to %q: %w", name, err)
		}
	}
	return nil
}
