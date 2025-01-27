#!/bin/bash -e

VERSION="$1"
if [ -z "$VERSION" ]; then
  echo "USAGE: $0 VERSION"
  exit 1
fi

OSs=(linux darwin)
ARCHs=(amd64 arm64)

for OS in "${OSs[@]}"; do
  for ARCH in "${ARCHs[@]}"; do
    echo Building "$OS/$ARCH"
    mkdir -p "build/$OS-$ARCH" .
    GOOS=$OS GOARCH=$ARCH go build -o "build/$OS-$ARCH/yab" .
    (cd "build/$OS-$ARCH" && zip "../yab-$VERSION-$OS-$ARCH.zip" yab)
  done
done
