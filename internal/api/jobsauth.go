package api

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	oidc "github.com/coreos/go-oidc/v3/oidc"

	"cdrom/internal/config"
)

// JobTokenAuth verifies the job tokens the API hands to execution targets and
// mints new ones (via the IdP) for dispatched jobs. A job token is a JWT
// scoped to a single job (its job_id, pipeline_id, and target group are in the
// claims) and to the API plus any outside resources a job may call (its
// audiences). An execution target presents the token back to the API on
// job-scoped calls (ReportJobStatus, artifact access) and to outside
// resources.
//
// The main job token's exp claim is a long placeholder (jobTokenLifetime):
// the token is meant to live as long as the job runs, so the API never
// enforces exp — it gates a token's validity on the job's live status in the
// database (see GRPCServer.checkJobToken). An exchanged token (a different
// audience) instead carries a short lifetime (the configured default, or the
// request's expires_in).
//
// It is nil when gRPC job-token auth is disabled, in which case the API's
// gRPC surface is open and no tokens are minted or checked.
type JobTokenAuth struct {
	verifier  *oidc.IDTokenVerifier
	audiences []string
	idpURL    string // base URL of the IdP, e.g. http://127.0.0.1:7104
	logger    *slog.Logger
	http      *http.Client
	// exchangedTokenLifetime is the default lifetime of an exchanged job token
	// (one a job requests for a different audience, e.g. an outside resource).
	// Unlike the main job token, an exchanged token is a scoped credential for
	// an outside resource and SHOULD expire. It is set from the config's
	// grpc_auth.exchanged_token_lifetime (defaulting to 15 minutes); a job may
	// override it per request via ExchangeJobToken's expires_in.
	exchangedTokenLifetime time.Duration
}

// jobTokenLifetime is how long the main job token is valid for. It is a long
// placeholder (a week): the API never enforces the token's exp claim (it
// gates validity on the job's live status in the database), so the value only
// matters to outside resources that verify the token with standard OIDC
// checks. A week is long enough that a job running for days is not rejected
// for an "expired" token.
const jobTokenLifetime = 7 * 24 * time.Hour

// NewJobTokenAuth builds the job-token verifier (via OIDC discovery against
// the IdP) and the mint client. It performs a network call to the IdP. The
// client is used for both discovery and minting; when it is an mTLS client
// (see grpcutil.HTTPClient) the API authenticates to the IdP's job-token
// endpoint with its client certificate. A nil client uses the default.
func NewJobTokenAuth(ctx context.Context, cfg config.GRPCAuthConfig, client *http.Client, logger *slog.Logger) (*JobTokenAuth, error) {
	if logger == nil {
		logger = slog.Default()
	}
	if client == nil {
		client = http.DefaultClient
	}
	base := normalizeIDPURL(cfg.IdPAddress)
	provider, err := oidc.NewProvider(oidc.ClientContext(ctx, client), base)
	if err != nil {
		return nil, fmt.Errorf("api: oidc discovery against idp %q: %w", base, err)
	}
	// Job tokens are not issued to a client, so the built-in client/audience
	// check is skipped; Verify validates the audience claim against the
	// configured audiences (a token is valid for the API when its aud contains
	// any of them). Expiry is also skipped: a job token's exp claim is a long
	// placeholder (the token is meant to live as long as the job runs, which
	// can be days), so the API gates a token's validity on the job's live
	// status in the database (see GRPCServer.checkJobToken) rather than on
	// exp.
	verifier := provider.Verifier(&oidc.Config{SkipClientIDCheck: true, SkipExpiryCheck: true})
	return &JobTokenAuth{
		verifier:               verifier,
		audiences:              cfg.Audiences,
		idpURL:                 base,
		logger:                 logger,
		http:                   client,
		exchangedTokenLifetime: cfg.EffectiveExchangedTokenLifetime(),
	}, nil
}

// Enabled reports whether job-token auth is active.
func (a *JobTokenAuth) Enabled() bool { return a != nil && a.verifier != nil }

// Verify validates token (signature, issuer, and audience) and returns the job
// ID it is scoped to. Expiry is not checked: a job token's exp claim is a long
// placeholder (the token is meant to live as long as the job runs), so the
// caller gates the token's validity on the job's live status in the database
// (see GRPCServer.checkJobToken) rather than on exp.
func (a *JobTokenAuth) Verify(ctx context.Context, token string) (int64, error) {
	idt, err := a.verifier.Verify(ctx, token)
	if err != nil {
		return 0, fmt.Errorf("api: verify job token: %w", err)
	}
	if !a.audienceOK(idt.Audience) {
		return 0, fmt.Errorf("api: job token audience %v not accepted", idt.Audience)
	}
	var claims struct {
		JobID string `json:"job_id"`
	}
	if err := idt.Claims(&claims); err != nil {
		return 0, fmt.Errorf("api: read job token claims: %w", err)
	}
	jobID, err := strconv.ParseInt(claims.JobID, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("api: job token has no valid job_id: %w", err)
	}
	return jobID, nil
}

// audienceOK reports whether the token's audience contains any of the
// configured audiences.
func (a *JobTokenAuth) audienceOK(aud []string) bool {
	for _, want := range a.audiences {
		for _, got := range aud {
			if got == want {
				return true
			}
		}
	}
	return false
}

// Mint requests the main job token for job from the IdP. The API
// authenticates to the IdP with its mTLS client certificate (when the IdP
// serves TLS). The token carries the IdP's default job-token audiences, the
// trigger that started the run (triggerName/triggerType), and the upstream
// OIDC claims a webhook caller presented (prefixed with upstream_). The
// upstream claims are the token's full claims as a JSON object, so a claim
// that is itself an object or a list (e.g. GitLab's "user_identities" or
// "job_config") keeps its structure on the minted token. The token's exp is a
// long placeholder (jobTokenLifetime): the API gates its validity on the
// job's live status, not the exp claim.
func (a *JobTokenAuth) Mint(ctx context.Context, jobID, pipelineID int64, targetGroup, triggerName, triggerType string, upstreamClaims map[string]any) (string, error) {
	return a.mint(ctx, jobID, pipelineID, targetGroup, triggerName, triggerType, upstreamClaims, "", jobTokenLifetime)
}

// Exchange requests a new job token for job from the IdP with a specific
// audience (e.g. an outside resource the job needs to call). The API
// authenticates to the IdP with its mTLS client certificate. The token carries
// the same trigger and upstream claims as the job's original token. Unlike the
// main job token, an exchanged token has a short lifetime: expiresIn (or the
// configured default when zero), since it is a scoped credential for an
// outside resource rather than the job's own long-lived token.
func (a *JobTokenAuth) Exchange(ctx context.Context, jobID, pipelineID int64, targetGroup, triggerName, triggerType, audience string, upstreamClaims map[string]any, expiresIn time.Duration) (string, error) {
	if expiresIn <= 0 {
		expiresIn = a.exchangedTokenLifetime
	}
	return a.mint(ctx, jobID, pipelineID, targetGroup, triggerName, triggerType, upstreamClaims, audience, expiresIn)
}

// mint requests a job token from the IdP's job_token grant. When audience is
// non-empty the token is stamped for that audience; otherwise the IdP uses
// its default job-token audiences. The trigger that started the run and the
// upstream OIDC claims a webhook caller presented (JSON-encoded, preserving
// each claim's structure) are passed so the IdP can stamp them onto the token.
// expiresIn is the token's lifetime (the IdP stamps exp to now + expiresIn);
// it is a long placeholder for the main job token and a short window for an
// exchanged token.
func (a *JobTokenAuth) mint(ctx context.Context, jobID, pipelineID int64, targetGroup, triggerName, triggerType string, upstreamClaims map[string]any, audience string, expiresIn time.Duration) (string, error) {
	form := url.Values{
		"grant_type": {"job_token"},
		"job_id":     {strconv.FormatInt(jobID, 10)},
	}
	if pipelineID != 0 {
		form.Set("pipeline_id", strconv.FormatInt(pipelineID, 10))
	}
	if targetGroup != "" {
		form.Set("target_group", targetGroup)
	}
	if triggerName != "" {
		form.Set("trigger_name", triggerName)
	}
	if triggerType != "" {
		form.Set("trigger_type", triggerType)
	}
	if len(upstreamClaims) > 0 {
		if encoded, err := json.Marshal(upstreamClaims); err == nil {
			form.Set("upstream_claims", string(encoded))
		}
	}
	if audience != "" {
		form.Set("audience", audience)
	}
	if expiresIn > 0 {
		form.Set("expires_in", expiresIn.String())
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, a.idpURL+"/token", strings.NewReader(form.Encode()))
	if err != nil {
		return "", fmt.Errorf("api: build mint request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := a.http.Do(req)
	if err != nil {
		return "", fmt.Errorf("api: mint job token: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("api: mint job token: status %d", resp.StatusCode)
	}
	var out struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return "", fmt.Errorf("api: decode mint response: %w", err)
	}
	if out.AccessToken == "" {
		return "", fmt.Errorf("api: mint response has no access_token")
	}
	return out.AccessToken, nil
}

// normalizeIDPURL ensures the IdP address is a full URL with a scheme, so it
// can be used for OIDC discovery and the mint request. A bare host:port is
// treated as http.
func normalizeIDPURL(addr string) string {
	if addr == "" {
		return addr
	}
	if strings.HasPrefix(addr, "http://") || strings.HasPrefix(addr, "https://") {
		return addr
	}
	return "http://" + addr
}
