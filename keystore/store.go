package keystore

import (
	"github.com/pkg/errors"
)

var ErrEmptyKeyStore = errors.New("empty keystore, unable to find any keys")

type (
	Key = string

	// Store is an interface that support issue of a key and rotation of currently being used key
	// through Rotate and Get apis.
	Store interface {
		Rotate()
		Get() Key
	}
)
