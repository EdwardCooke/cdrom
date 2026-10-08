package idp

import (
	"encoding/json"
	"fmt"

	jose "github.com/go-jose/go-jose/v4"
)

// jwk renders the key's public part as a JSON Web Key for the JWKS the IdP
// serves. The key ID, algorithm, and use are stamped so a verifier can select
// the right key and confirm the signature algorithm.
func (k *signingKey) jwk() jose.JSONWebKey {
	return jose.JSONWebKey{
		Key:       k.key.Public(),
		KeyID:     k.kid,
		Algorithm: string(jose.RS256),
		Use:       "sig",
	}
}

// signRS256 builds a compact RS256 JWT over claims, signed with the key. The
// JWT header carries the key ID (so a verifier selects the matching JWKS key)
// and the "JWT" type.
func (k *signingKey) signRS256(claims map[string]any) (string, error) {
	payload, err := json.Marshal(claims)
	if err != nil {
		return "", fmt.Errorf("idp: marshal jwt claims: %w", err)
	}
	signer, err := jose.NewSigner(jose.SigningKey{
		Algorithm: jose.RS256,
		Key: &jose.JSONWebKey{
			Key:       k.key,
			KeyID:     k.kid,
			Algorithm: string(jose.RS256),
		},
	}, (&jose.SignerOptions{}).WithType(jose.ContentType("JWT")))
	if err != nil {
		return "", fmt.Errorf("idp: create jwt signer: %w", err)
	}
	obj, err := signer.Sign(payload)
	if err != nil {
		return "", fmt.Errorf("idp: sign jwt: %w", err)
	}
	token, err := obj.CompactSerialize()
	if err != nil {
		return "", fmt.Errorf("idp: serialize jwt: %w", err)
	}
	return token, nil
}
