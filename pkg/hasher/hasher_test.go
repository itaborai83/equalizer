package hasher

import (
	"testing"
)

func TestHasher(t *testing.T) {
	h := NewHasher()

	t.Run("Update and GetHash with various data types", func(t *testing.T) {
		h.Reset()
		h.Update(42)
		hash, err := h.GetHash()
		if err != nil {
			t.Errorf("GetHash() error = %v; want nil", err)
		}
		expectedHash := uint64(1048987256713788730)
		if hash != expectedHash {
			t.Errorf("GetHash() = %d; want %d", hash, expectedHash)
		}

		h.Reset()
		h.Update("hello")
		hash, err = h.GetHash()
		if err != nil {
			t.Errorf("GetHash() error = %v; want nil", err)
		}
		expectedHashString := uint64(4423936489020281466)
		if hash != expectedHashString {
			t.Errorf("GetHash() = %d; want %d", hash, expectedHashString)
		}

		h.Reset()
		h.Update(3.14)
		hash, err = h.GetHash()
		if err != nil {
			t.Errorf("GetHash() error = %v; want nil", err)
		}
		expectedHashFloat := uint64(7020983630915418268)
		if hash != expectedHashFloat {
			t.Errorf("GetHash() = %d; want %d", hash, expectedHashFloat)
		}

		h.Reset()
		h.Update(true)
		hash, err = h.GetHash()
		if err != nil {
			t.Errorf("GetHash() error = %v; want nil", err)
		}
		expectedHashBool := uint64(17265805410231011923)
		if hash != expectedHashBool {
			t.Errorf("GetHash() = %d; want %d", hash, expectedHashBool)
		}

		h.Reset()
		h.Update(nil)
		hash, err = h.GetHash()
		if err != nil {
			t.Errorf("GetHash() error = %v; want nil", err)
		}
		expectedHashNil := uint64(12608345512199880753)
		if hash != expectedHashNil {
			t.Errorf("GetHash() = %d; want %d", hash, expectedHashNil)
		}
	})

	t.Run("Invalid GetHash() when no data is updated", func(t *testing.T) {
		h.Reset()
		_, err := h.GetHash()
		expectedError := "data is empty"
		if err == nil || err.Error() != expectedError {
			t.Errorf("GetHash() error = %v; want %s", err, expectedError)
		}
	})

	t.Run("Panic on unsupported data type", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("Update() did not panic on unsupported type")
			}
		}()
		h.Reset()
		h.Update(struct{}{}) // Pass unsupported type to trigger panic
	})
}
