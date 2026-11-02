#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
case "$(uname -s)-$(uname -m)" in
  Darwin-arm64) platform=osx-aarch_64 ;;
  Darwin-x86_64) platform=osx-x86_64 ;;
  Linux-x86_64) platform=linux-x86_64 ;;
  Linux-aarch64) platform=linux-aarch_64 ;;
  *) echo 'Unsupported platform. Install protoc 33.0 manually.' >&2; exit 1 ;;
esac
mkdir -p .tools
archive=$(mktemp)
trap 'rm -f "$archive"' EXIT
curl -fsSL "https://github.com/protocolbuffers/protobuf/releases/download/v33.0/protoc-33.0-${platform}.zip" -o "$archive"
unzip -qo "$archive" -d .tools/protoc
GOBIN="$PWD/.tools" go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.12
GOBIN="$PWD/.tools" go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.6.2
