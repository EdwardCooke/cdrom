package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

// IDPMetrics is the IdP's metric set (F-20). It is constructed against a
// Collector's registry (NewIDPMetrics) and records token minting (ui / job /
// exchanged), username/password sign-ins (success / invalid_password /
// locked), the signing-keyring size, key rotations, and the user directory
// size.
//
// Every method is nil-safe: when metrics are disabled the IdP passes a nil
// *IDPMetrics and all recording is a no-op.
type IDPMetrics struct {
	// TokensMinted counts tokens minted by kind (ui / job / exchanged).
	TokensMinted *prometheus.CounterVec
	// Logins counts username/password sign-ins by outcome (success /
	// invalid_password / locked).
	Logins *prometheus.CounterVec
	// Keys is the number of signing keys in the keyring.
	Keys prometheus.Gauge
	// KeyRotations counts signing-key rotations.
	KeyRotations prometheus.Counter
	// Users is the user directory's size.
	Users prometheus.Gauge
}

// NewIDPMetrics builds the IdP metric set, registering its collectors with
// reg.
func NewIDPMetrics(reg prometheus.Registerer) *IDPMetrics {
	m := &IDPMetrics{
		TokensMinted: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: Prefix + "_idp_tokens_minted_total",
			Help: "IdP tokens minted, by kind (ui / job / exchanged).",
		}, []string{"kind"}),
		Logins: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: Prefix + "_idp_logins_total",
			Help: "Username/password sign-ins, by outcome.",
		}, []string{"outcome"}),
		Keys: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: Prefix + "_idp_keys",
			Help: "Signing keys in the keyring.",
		}),
		KeyRotations: prometheus.NewCounter(prometheus.CounterOpts{
			Name: Prefix + "_idp_key_rotations_total",
			Help: "Signing-key rotations.",
		}),
		Users: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: Prefix + "_idp_users",
			Help: "User directory size.",
		}),
	}
	reg.MustRegister(m.TokensMinted, m.Logins, m.Keys, m.KeyRotations, m.Users)
	return m
}

// RecordTokenMinted counts a token minted by kind (ui / job / exchanged).
func (m *IDPMetrics) RecordTokenMinted(kind string) {
	if m == nil || kind == "" {
		return
	}
	m.TokensMinted.WithLabelValues(kind).Inc()
}

// RecordLogin counts a username/password sign-in by outcome.
func (m *IDPMetrics) RecordLogin(outcome string) {
	if m == nil || outcome == "" {
		return
	}
	m.Logins.WithLabelValues(outcome).Inc()
}

// SetKeys sets the signing-keyring size gauge.
func (m *IDPMetrics) SetKeys(n int) {
	if m == nil {
		return
	}
	m.Keys.Set(float64(n))
}

// RecordKeyRotation counts a signing-key rotation.
func (m *IDPMetrics) RecordKeyRotation() {
	if m == nil {
		return
	}
	m.KeyRotations.Inc()
}

// SetUsers sets the user directory size gauge.
func (m *IDPMetrics) SetUsers(n int) {
	if m == nil {
		return
	}
	m.Users.Set(float64(n))
}
