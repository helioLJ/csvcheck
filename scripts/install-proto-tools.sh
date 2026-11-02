#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
case "$(uname -s)-$(uname -m)" in
  Darwin-arm64) platform=osx-aarch_64; checksum=3cf55dd47118bd2efda9cd26b74f8bbbfcf5beb1bf606bc56ad4c001b543f6d3 ;;
  Darwin-x86_64) platform=osx-x86_64; checksum=e4e50a703147a92d1a5a2d3a34c9e41717f67ade67d4be72b9a466eb8f22fe87 ;;
  Linux-x86_64) platform=linux-x86_64; checksum=d99c011b799e9e412064244f0be417e5d76c9b6ace13a2ac735330fa7d57ad8f ;;
  Linux-aarch64) platform=linux-aarch_64; checksum=4b96bc91f8b54d829b8c3ca2207ff1ceb774843321e4fa5a68502faece584272 ;;
  *) echo 'Unsupported platform. Install protoc 33.0 manually.' >&2; exit 1 ;;
esac
mkdir -p .tools
archive=$(mktemp)
trap 'rm -f "$archive"' EXIT
curl -fsSL "https://github.com/protocolbuffers/protobuf/releases/download/v33.0/protoc-33.0-${platform}.zip" -o "$archive"
actual=$(shasum -a 256 "$archive" | cut -d " " -f 1)
if [ "$actual" != "$checksum" ]; then
  echo "Protobuf archive checksum mismatch." >&2
  exit 1
fi
unzip -qo "$archive" -d .tools/protoc
GOBIN="$PWD/.tools" go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.12
GOBIN="$PWD/.tools" go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.6.2
