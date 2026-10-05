// Package tokenexchange provides the API-backed implementation of the
// executor's TokenExchange interface: it lets a step handler request a new job
// token for a different audience (e.g. an outside resource the job needs to
// call) by calling the API's ExchangeJobToken RPC, presenting the job's own
// token.
//
// Both the long-lived worker and the ephemeral agent use it: each sets it in
// the executor's context (via executor.ContextWithTokenExchange) before running
// a job's spec, so a step handler (the built-in "token_exchange" handler) can
// obtain an exchanged token. It is the single shared implementation, so the
// worker and agent do not each carry a copy.
package tokenexchange

import (
	"context"
	"time"

	apipb "cdrom/internal/gen/cdrom/api/v1"
	"cdrom/internal/grpcutil"

	"google.golang.org/protobuf/types/known/durationpb"
)

// Exchanger is the API-backed implementation of executor.TokenExchange. It
// calls the API's ExchangeJobToken RPC, presenting the job's own token, and
// returns the exchanged token.
type Exchanger struct {
	api   apipb.APIClient
	jobID int64
	token string
}

// New builds an Exchanger for job, presenting token on its API calls.
func New(api apipb.APIClient, jobID int64, token string) *Exchanger {
	return &Exchanger{api: api, jobID: jobID, token: token}
}

// Exchange implements executor.TokenExchange. It requests a new job token
// scoped to the same job but the given audience. expiresIn is how long the
// exchanged token should be valid for; a zero value leaves it to the API's
// default exchanged-token lifetime.
func (e *Exchanger) Exchange(ctx context.Context, audience string, expiresIn time.Duration) (string, error) {
	resp, err := e.api.ExchangeJobToken(grpcutil.WithBearerToken(ctx, e.token), &apipb.ExchangeJobTokenRequest{
		JobId:     e.jobID,
		Audience:  audience,
		ExpiresIn: durationpb.New(expiresIn),
	})
	if err != nil {
		return "", err
	}
	return resp.GetToken(), nil
}
