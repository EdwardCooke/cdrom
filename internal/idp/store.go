package idp

import (
	dbpb "cdrom/internal/gen/cdrom/db/v1"
)

// NewDBKeyStore returns a KeyStore that persists the signing keyring through
// the Database service.
func NewDBKeyStore(db dbpb.DatabaseClient) KeyStore {
	return &dbKeyStore{db: db}
}

// NewDBAuthCodeStore returns an AuthCodeStore that persists OIDC
// authorization codes through the Database service.
func NewDBAuthCodeStore(db dbpb.DatabaseClient) AuthCodeStore {
	return &dbAuthCodeStore{db: db}
}
