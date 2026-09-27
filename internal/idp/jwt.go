package idp

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/big"
)

// jwkJSON is a single entry in a JSON Web Key Set (JWKS). Only the fields
// needed to verify an RS256 signature are emitted.
type jwkJSON struct {
	Kty string `json:"kty"`
	Use string `json:"use"`
	Alg string `json:"alg"`
	Kid string `json:"kid"`
	N   string `json:"n"`
	E   string `json:"e"`
}

// jwk renders the key's public part as a JWKS entry.
func (k *signingKey) jwk() jwkJSON {
	return jwkJSON{
		Kty: "RSA",
		Use: "sig",
		Alg: "RS256",
		Kid: k.kid,
		N:   base64.RawURLEncoding.EncodeToString(k.key.PublicKey.N.Bytes()),
		E:   base64.RawURLEncoding.EncodeToString(big.NewInt(int64(k.key.PublicKey.E)).Bytes()),
	}
}

// signRS256 builds a compact RS256 JWT over claims, signed with the key.
func (k *signingKey) signRS256(claims map[string]any) (string, error) {
	header, err := json.Marshal(map[string]any{"alg": "RS256", "typ": "JWT", "kid": k.kid})
	if err != nil {
		return "", fmt.Errorf("idp: marshal jwt header: %w", err)
	}
	payload, err := json.Marshal(claims)
	if err != nil {
		return "", fmt.Errorf("idp: marshal jwt claims: %w", err)
	}
	signingInput := base64.RawURLEncoding.EncodeToString(header) + "." +
		base64.RawURLEncoding.EncodeToString(payload)
	hashed := sha256.Sum256([]byte(signingInput))
	sig, err := rsa.SignPKCS1v15(rand.Reader, k.key, crypto.SHA256, hashed[:])
	if err != nil {
		return "", fmt.Errorf("idp: sign jwt: %w", err)
	}
	return signingInput + "." + base64.RawURLEncoding.EncodeToString(sig), nil
}
