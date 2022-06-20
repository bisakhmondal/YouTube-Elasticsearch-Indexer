package keystore

// InMemoryStore implements Store interface and provides an in memory version of the key store.
type InMemoryStore struct {
	keys         []Key
	currentKeyId int
}

// NewInMemoryKeyStore constructor to create a key store
func NewInMemoryKeyStore(keys []Key) (*InMemoryStore, error) {
	if len(keys) == 0 {
		return nil, ErrEmptyKeyStore
	}

	return &InMemoryStore{
		keys:         keys,
		currentKeyId: 0,
	}, nil
}

func (i *InMemoryStore) Rotate() {
	// next key modulo total size of key store
	i.currentKeyId = (i.currentKeyId + 1) % len(i.keys)
}

func (i *InMemoryStore) Get() Key {
	return i.keys[i.currentKeyId]
}
