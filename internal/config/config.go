// Package config reads environment configuration at process startup.
package config

import (
	"os"
	"time"
)

func Env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
func Duration(key string, fallback time.Duration) (time.Duration, error) {
	if v := os.Getenv(key); v != "" {
		return time.ParseDuration(v)
	}
	return fallback, nil
}
