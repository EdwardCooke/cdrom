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

	oidc "github.com/coreos/go-oidc/v3/oidc"

	"cdrom/internal/config"
)

// JobTokenAuth verifies the job tokens the API hands to execution targets and
// mints new ones (via the IdP) for dispatched jobs. A job token is a short-
// lived JWT scoped to a single job (its job_id, pipeline_id, and target group
// are in the claims) and to the API plus any outside resources a job may call
// (its audiences). An execution target presents the token back to the API on
// job-scoped calls (ReportJobStatus, artifact access) and to outside resources.
//
// It is nil when gRPC job-token auth is disabled, in which case the API's
// gRPC surface is open and no tokens are minted or checked.
type JobTokenAuth struct {
	verifier  *oidc.IDTokenVerifier
	audiences []string
	idpURL    string // base URL of the IdP, e.g. http://127.0.0.1:7104
	logger    *slog.Logger
	http      *http.Client
}

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
	// any of them).
	verifier := provider.Verifier(&oidc.Config{SkipClientIDCheck: true})
	return &JobTokenAuth{
		verifier:  verifier,
		audiences: cfg.Audiences,
		idpURL:    base,
		logger:    logger,
		http:      client,
	}, nil
}

// Enabled reports whether job-token auth is active.
func (a *JobTokenAuth) Enabled() bool { return a != nil && a.verifier != nil }

// Verify validates token (signature, issuer, expiry, and audience) and returns
// the job ID it is scoped to.
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

// Mint requests a job token for job from the IdP. The API authenticates to
// the IdP with its mTLS client certificate (when the IdP serves TLS). The
// token carries the IdP's default job-token audiences, the trigger that
// started the run (triggerName/triggerType), and the upstream OIDC claims a
// webhook caller presented (prefixed with upstream_). The upstream claims are
// the token's full claims as a JSON object, so a claim that is itself an
// object or a list (e.g. GitLab's "user_identities" or "job_config") keeps
// its structure on the minted token.
func (a *JobTokenAuth) Mint(ctx context.Context, jobID, pipelineID int64, targetGroup, triggerName, triggerType string, upstreamClaims map[string]any) (string, error) {
	return a.mint(ctx, jobID, pipelineID, targetGroup, triggerName, triggerType, upstreamClaims, "")
}

// Exchange requests a new job token for job from the IdP with a specific
// audience (e.g. an outside resource the job needs to call). The API
// authenticates to the IdP with its mTLS client certificate. The token carries
// the same trigger and upstream claims as the job's original token.
func (a *JobTokenAuth) Exchange(ctx context.Context, jobID, pipelineID int64, targetGroup, triggerName, triggerType, audience string, upstreamClaims map[string]any) (string, error) {
	return a.mint(ctx, jobID, pipelineID, targetGroup, triggerName, triggerType, upstreamClaims, audience)
}

// mint requests a job token from the IdP's job_token grant. When audience is
// non-empty the token is stamped for that audience; otherwise the IdP uses
// its default job-token audiences. The trigger that started the run and the
// upstream OIDC claims a webhook caller presented (JSON-encoded, preserving
// each claim's structure) are passed so the IdP can stamp them onto the token.
func (a *JobTokenAuth) mint(ctx context.Context, jobID, pipelineID int64, targetGroup, triggerName, triggerType string, upstreamClaims map[string]any, audience string) (string, error) {
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
