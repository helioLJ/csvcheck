package storage

import (
	"context"
	"errors"
	"io"
	"os"
	"strings"
	"testing"
)

type brokenReader struct{}

func (brokenReader) Read([]byte) (int, error) { return 0, errors.New("connection lost") }
func TestAtomicPublication(t *testing.T) {
	store, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err = store.Put(context.Background(), "inputs/test.csv", brokenReader{}); err == nil {
		t.Fatal("expected failure")
	}
	if _, err = store.Open("inputs/test.csv"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("partial upload visible: %v", err)
	}
	n, hash, err := store.Put(context.Background(), "inputs/test.csv", strings.NewReader("hello"))
	if err != nil {
		t.Fatal(err)
	}
	if n != 5 || hash != "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824" {
		t.Fatal(n, hash)
	}
	f, err := store.Open("inputs/test.csv")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	b, _ := io.ReadAll(f)
	if string(b) != "hello" {
		t.Fatal(string(b))
	}
}
func TestRejectPathTraversal(t *testing.T) {
	store, _ := New(t.TempDir())
	for _, key := range []string{"../secret", "/tmp/secret", "inputs/../../secret", "a\\b", "a/./b", ""} {
		if _, err := store.Path(key); err == nil {
			t.Fatalf("accepted %q", key)
		}
	}
}
