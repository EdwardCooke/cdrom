package api

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"strings"
	"sync"

	oidc "github.com/coreos/go-oidc/v3/oidc"
	"google.golang.org/protobuf/types/known/structpb"
)

// httpBearerToken returns the token from the request's Authorization header,
// or "" when the header is absent or is not a Bearer token.
func httpBearerToken(r *http.Request) string {
	header := r.Header.Get("Authorization")
	const prefix = "Bearer "
	if len(header) < len(prefix) || !strings.EqualFold(header[:len(prefix)], prefix) {
		return ""
	}
	return strings.TrimSpace(header[len(prefix):])
}

// webhookTokenVerifier verifies a Bearer token presented to a webhook trigger
// against an OIDC issuer and returns the token's claims as a JSON object
// (a claim that is itself an object or a list keeps its structure). The
// production implementation (webhookOIDCVerifier) performs OIDC discovery and
// JWKS verification; tests substitute a fake so the claim-matching path can be
// exercised without a live identity provider.
type webhookTokenVerifier interface {
	verifyToken(ctx context.Context, issuer, token string) (map[string]any, error)
}

// webhookOIDCVerifier is the production webhookTokenVerifier. It caches an
// OIDC verifier per issuer so a webhook trigger's oidc_issuer is discovered
// (and its JWKS fetched) once, not on every request. It uses the default HTTP
// client, so it reaches external providers (GitHub, GitLab) over HTTPS and a
// plaintext local IdP.
type webhookOIDCVerifier struct {
	mu    sync.Mutex
	cache map[string]*oidc.IDTokenVerifier
}

// newWebhookOIDCVerifier returns an empty verifier cache.
func newWebhookOIDCVerifier() *webhookOIDCVerifier {
	return &webhookOIDCVerifier{cache: make(map[string]*oidc.IDTokenVerifier)}
}

// verifierFor returns a verifier for issuer, building (and caching) it on
// first use. It performs OIDC discovery (a network call) the first time an
// issuer is seen.
func (v *webhookOIDCVerifier) verifierFor(ctx context.Context, issuer string) (*oidc.IDTokenVerifier, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if verifier, ok := v.cache[issuer]; ok {
		return verifier, nil
	}
	provider, err := oidc.NewProvider(ctx, issuer)
	if err != nil {
		return nil, fmt.Errorf("api: oidc discovery %q: %w", issuer, err)
	}
	// A webhook token is not issued to a client, so the built-in client/
	// audience check is skipped; the trigger's oidc_claims gate the request.
	verifier := provider.Verifier(&oidc.Config{SkipClientIDCheck: true})
	v.cache[issuer] = verifier
	return verifier, nil
}

// verifyToken verifies token against issuer and returns its claims as a JSON
// object (a claim that is itself an object or a list keeps its structure).
// The caller flattens the claims (flattenClaims) when matching them against a
// trigger's oidc_claims, and records the raw structured claims as the run's
// upstream claims.
func (v *webhookOIDCVerifier) verifyToken(ctx context.Context, issuer, token string) (map[string]any, error) {
	verifier, err := v.verifierFor(ctx, issuer)
	if err != nil {
		return nil, err
	}
	idToken, err := verifier.Verify(ctx, token)
	if err != nil {
		return nil, fmt.Errorf("api: verify webhook token: %w", err)
	}
	var claims map[string]any
	if err := idToken.Claims(&claims); err != nil {
		return nil, fmt.Errorf("api: read webhook token claims: %w", err)
	}
	return claims, nil
}

// flattenClaims flattens a (possibly nested) set of OIDC claims into a
// dot-addressed map of string values: a nested object {a: {b: "x"}} becomes
// {"a.b": "x"}. This lets a trigger's oidc_claims address a claim that a
// provider nests (e.g. GitLab's "resource_access") with a dot.
func flattenClaims(claims map[string]any) map[string]string {
	out := make(map[string]string, len(claims))
	flattenIntoClaims("", claims, out)
	return out
}

func flattenIntoClaims(prefix string, claims map[string]any, out map[string]string) {
	for k, v := range claims {
		key := k
		if prefix != "" {
			key = prefix + "." + k
		}
		if nested, ok := v.(map[string]any); ok {
			flattenIntoClaims(key, nested, out)
			continue
		}
		out[key] = renderClaimValue(v)
	}
}

// renderClaimValue renders a scalar claim value as a string so it is
// comparable against a trigger's expected value. A list-valued claim is
// rendered as its JSON encoding.
func renderClaimValue(v any) string {
	switch val := v.(type) {
	case nil:
		return ""
	case string:
		return val
	case float64:
		if val == math.Trunc(val) {
			return strconv.FormatInt(int64(val), 10)
		}
		return strconv.FormatFloat(val, 'f', -1, 64)
	case bool:
		return strconv.FormatBool(val)
	default:
		b, _ := json.Marshal(v)
		return string(b)
	}
}

// oidcClaimsMatch reports whether claims satisfies required: for each
// required claim name, a value of "*" is a wildcard that matches any value
// (including the claim being absent); any other value must equal the claim's
// value (the claim must be present).
func oidcClaimsMatch(claims, required map[string]string) bool {
	for name, want := range required {
		if want == "*" {
			continue
		}
		if got, ok := claims[name]; !ok || got != want {
			return false
		}
	}
	return true
}

// upstreamClaimsToStruct converts a webhook caller's structured claims (a JSON
// object of arbitrary values) into a protobuf Struct so they can cross the
// gRPC boundary to the scheduler/database without flattening complex values
// (objects, lists) to strings. A nil or empty map yields a nil Struct (the
// proto's "absent" representation).
func upstreamClaimsToStruct(claims map[string]any) *structpb.Struct {
	if len(claims) == 0 {
		return nil
	}
	s, err := structpb.NewStruct(claims)
	if err != nil {
		// A claim value that is not JSON-encodable would fail here; that
		// cannot happen for OIDC claims (JSON scalars, objects, or lists).
		return &structpb.Struct{Fields: map[string]*structpb.Value{}}
	}
	return s
}

// upstreamClaimsFromStruct converts a protobuf Struct of upstream claims back
// to a JSON-object map (nil for an absent/empty struct). Each field's value is
// decoded to its Go representation (a nested object becomes a map, a list a
// slice, a number a float64), so a complex claim keeps its structure.
func upstreamClaimsFromStruct(s *structpb.Struct) map[string]any {
	if s == nil || len(s.GetFields()) == 0 {
		return nil
	}
	out := make(map[string]any, len(s.GetFields()))
	for k, v := range s.GetFields() {
		out[k] = v.AsInterface()
	}
	return out
}
