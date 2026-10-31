package api

import (
	"context"
	"net/http"

	"cdrom/internal/auth"
	dbpb "cdrom/internal/gen/cdrom/db/v1"
)

// WrapClaimObserver wraps an HTTP handler so that, for every request arriving
// with an authenticated principal that carries token claims (F-27), the
// principal's observed claims are recorded in the Database service. This is
// what powers approval groups whose MEMBERSHIP IS DERIVED FROM AN IdP CLAIM:
// the group's effective membership is the union of its manual members and
// every principal OBSERVED to carry the group's declared (claim, value) pair.
//
// Observation is OPPORTUNISTIC and BEST-EFFORT: it runs on the request path
// (wrapped INSIDE the auth middleware, so it sees the verified user) but never
// alters the response — a failure to record (the Database service is down, or
// the principal has no claims) is swallowed. A principal is considered to
// carry a claim value if ANY of its observed tokens does, so observing the
// decider's LIVE token at decision time is the authoritative source; the
// recorded observations merely broaden the pool to principals who have signed
// in recently.
//
// When the Database client is unavailable (local dev without the full stack)
// the wrapper is a transparent pass-through.
func WrapClaimObserver(h http.Handler, s *Server) http.Handler {
	if s == nil || s.clients.Database == nil {
		return h
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if user := auth.UserFromContext(r.Context()); len(user.Claims) > 0 {
			s.observeClaims(r.Context(), user)
		}
		h.ServeHTTP(w, r)
	})
}

// observeClaims records the principal's observed claims (claim name → values)
// in the Database service. Fire-and-forget: errors are logged, not propagated.
func (s *Server) observeClaims(ctx context.Context, user auth.User) {
	for name, values := range user.Claims {
		for _, value := range values {
			if _, err := s.clients.Database.RecordObservedClaim(ctx, &dbpb.RecordObservedClaimRequest{
				PrincipalSubject: user.Subject,
				ClaimName:        name,
				ClaimValue:       value,
			}); err != nil {
				s.logger.Debug("api: record observed claim", "subject", user.Subject, "claim", name, "err", err)
			}
		}
	}
}
