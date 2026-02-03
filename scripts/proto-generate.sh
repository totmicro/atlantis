#!/bin/bash
# Generate protobuf code from proto definitions
# This script is called by 'make proto-generate'

set -e

# Check if protoc is installed
if ! command -v protoc &> /dev/null; then
    echo "Error: protoc is required but not installed."
    echo "Install from: https://grpc.io/docs/protoc-installation/"
    exit 1
fi

# Install protoc-gen-go and protoc-gen-go-grpc if not present
echo "Installing/updating protoc plugins..."
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

# Ensure proto directory exists
if [ ! -d "proto" ]; then
    echo "Error: proto directory not found"
    exit 1
fi

# Generate Go code
echo "Generating Go code from proto files..."
protoc --go_out=. --go_opt=paths=source_relative \
    --go-grpc_out=. --go-grpc_opt=paths=source_relative \
    proto/*.proto

echo "✓ Protobuf code generated successfully"
echo "  Generated files in: server/core/proto/"
