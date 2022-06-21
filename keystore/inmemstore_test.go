package keystore

import (
	"testing"

	"github.com/pkg/errors"
)

func TestInMemoryStore(t *testing.T) {
	store, err := NewInMemoryKeyStore([]Key{"1", "2", "3"})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		expectedKey string
		rotateNext  bool
	}{
		{
			expectedKey: "1",
			rotateNext:  false,
		},
		{
			expectedKey: "1",
			rotateNext:  true,
		},
		{
			expectedKey: "2",
			rotateNext:  true,
		}, {
			expectedKey: "3",
			rotateNext:  true,
		},
		{
			expectedKey: "1",
			rotateNext:  false,
		},
	} {
		if store.Get() != tc.expectedKey {
			t.Fatalf("expected %s, got %s", tc.expectedKey, store.Get())
		}

		if tc.rotateNext {
			store.Rotate()
		}
	}
}

func TestErrEmptyStore(t *testing.T) {
	_, err := NewInMemoryKeyStore([]Key{})
	if !errors.Is(err, ErrEmptyKeyStore) {
		t.Fatalf("expected %v, received %v", ErrEmptyKeyStore, err)
	}
}
