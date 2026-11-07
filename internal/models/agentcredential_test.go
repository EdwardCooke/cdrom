package models

import (
	"testing"
	"time"
)

// TestAgentCredentialActiveMatrix nails the liveness predicate (F-30.2): the
// row — not the JWT — decides life. Active iff not revoked, not retired
// (superseded), and before the deadline (a zero deadline never lapses).
func TestAgentCredentialActiveMatrix(t *testing.T) {
	now := time.Now().UTC()
	expired := now.Add(-time.Hour)
	active := now.Add(time.Hour)

	cases := []struct {
		name      string
		asOf      time.Time
		expiresAt time.Time
		revoked   bool
		replaced  string
		want      bool
	}{
		{"baseline active", now, active, false, "", true},
		{"past deadline", now, expired, false, "", false},
		{"zero deadline never lapses", now, time.Time{}, false, "", true},
		{"revoked kills activity", now, active, true, "", false},
		{"retired (replaced) kills activity", now, active, false, "succ-id", false},
		{"revoked AND retired AND expired", now, expired, true, "succ-id", false},
		{"instant-before-deadline-active", active.Add(-nanoseconds()), active, false, "", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := &AgentCredential{ExpiresAt: tc.expiresAt, ReplacedByID: tc.replaced}
			if tc.revoked {
				r := tc.asOf
				c.RevokedAt = &r
			}
			if got := c.Active(tc.asOf); got != tc.want {
				t.Errorf("Active(%v) = %v, want %v", tc.asOf, got, tc.want)
			}
		})
	}
}

// nanoseconds returns a 1 ns duration (helper to dodge import sprawl).
func nanoseconds() time.Duration { return time.Nanosecond }
