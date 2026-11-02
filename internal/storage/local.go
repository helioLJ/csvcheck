// Package storage provides atomic local file publication for the Compose demo.
package storage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
)

const MaxFileBytes int64 = 1 << 30

type Local struct{ Root string }

func New(root string) (*Local, error) {
	absolute, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	if err = os.MkdirAll(absolute, 0700); err != nil {
		return nil, err
	}
	return &Local{Root: absolute}, nil
}
func (l *Local) Path(key string) (string, error) {
	if key == "" || filepath.IsAbs(key) || strings.Contains(key, "\\") {
		return "", errors.New("invalid storage key")
	}
	clean := filepath.Clean(key)
	if clean != key || clean == ".." || strings.HasPrefix(clean, "../") {
		return "", errors.New("invalid storage key")
	}
	return filepath.Join(l.Root, clean), nil
}
func (l *Local) Open(key string) (*os.File, error) {
	p, err := l.Path(key)
	if err != nil {
		return nil, err
	}
	return os.Open(p)
}

// Put publishes a complete file by rename. Callers generate unique keys.
func (l *Local) Put(ctx context.Context, key string, source io.Reader) (size int64, hash string, err error) {
	path, err := l.Path(key)
	if err != nil {
		return 0, "", err
	}
	if err = os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return 0, "", err
	}
	temp, err := os.CreateTemp(filepath.Dir(path), ".upload-*")
	if err != nil {
		return 0, "", err
	}
	defer os.Remove(temp.Name())
	defer temp.Close()
	digest := sha256.New()
	buf := make([]byte, 64<<10)
	limited := io.LimitReader(source, MaxFileBytes+1)
	for {
		if err = ctx.Err(); err != nil {
			return 0, "", err
		}
		n, readErr := limited.Read(buf)
		if n > 0 {
			if _, err = io.MultiWriter(temp, digest).Write(buf[:n]); err != nil {
				return 0, "", err
			}
			size += int64(n)
		}
		if size > MaxFileBytes {
			return 0, "", errors.New("file exceeds 1 GiB limit")
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return 0, "", readErr
		}
	}
	if err = temp.Sync(); err != nil {
		return 0, "", err
	}
	if err = temp.Close(); err != nil {
		return 0, "", err
	}
	if err = os.Rename(temp.Name(), path); err != nil {
		return 0, "", err
	}
	return size, hex.EncodeToString(digest.Sum(nil)), nil
}
func (l *Local) Inspect(ctx context.Context, key string) (int64, string, error) {
	f, err := l.Open(key)
	if err != nil {
		return 0, "", err
	}
	defer f.Close()
	h := sha256.New()
	n, err := io.Copy(h, &contextReader{ctx, f})
	return n, hex.EncodeToString(h.Sum(nil)), err
}

type contextReader struct {
	ctx context.Context
	r   io.Reader
}

func (r *contextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.r.Read(p)
}
