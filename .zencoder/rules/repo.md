---
description: Repository Information Overview
alwaysApply: true
---

# simple-column-db Information

## Summary
A high-performance, column-oriented database implementation in Go. It features parallel query execution via chunked processing, optimized bit manipulation, and support for multiple compression schemes including LZ4, Snappy, and a custom Diff encoding.

## Structure
- **bits/**: Bitfield operations and byte-to-array transformation utilities.
- **compression/**: Implementations of LZ4, Snappy, and specialized Aligner/Diff encodings.
- **io/**: Low-level file I/O utilities for reading and dumping columnar data blocks.
- **lightsync/**: Concurrency and synchronization primitives like ring buffers and waiters.
- **lists/**: Optimized algorithms for list intersection and merging.
- **manager/**: Core database management, including query planning, execution, cache handling, and metadata.
- **ops/**: Comparison operations (EQ, GT, LT, RANGE) and a code generation framework for type-specific optimizations.
- **schema/**: Data type definitions, block headers, and slab management structures.
- **cmd/**: Command-line utilities, notably the `ops_codegen` tool for generating optimized operation code.

## Language & Runtime
**Language**: Go  
**Version**: 1.24.4  
**Build System**: Go Modules  
**Package Manager**: go mod

## Dependencies
**Main Dependencies**:
- `github.com/pierrec/lz4/v4`: LZ4 compression.
- `github.com/golang/snappy`: Snappy compression.
- `github.com/flosch/pongo2/v6`: Template engine used for code generation.
- `github.com/google/uuid`: UUID generation for database entities.
- `golang.org/x/sync`: Enhanced synchronization primitives.
- `github.com/fatih/color`: Colorized terminal output.

## Build & Installation
```bash
# Build the main database application
go build .

# Build the code generation tool
go build ./cmd/ops_codegen

# Run code generation (from the ops directory)
cd ops && go run ../cmd/ops_codegen/main.go
```

## Main Files & Resources
- **Entry Point**: `main.go` - The primary application entry point.
- **Codegen Tool**: `cmd/ops_codegen/main.go` - Tool that generates optimized filters based on `ops/codegen.config.json`.
- **Core Manager**: `manager/manager.go` - Orchestrates slabs, metadata, and query planning.
- **Executor**: `manager/executor/chunk_thread_processor.go` - Handles parallel processing of data chunks.

## Testing
**Framework**: Standard Go `testing` package.
- **Test Location**: Located alongside source files (e.g., `compression/diff_test.go`, `intersect_test.go`).
- **Naming Convention**: Files ending in `_test.go`.
- **Benchmarks**: Extensive use of Go benchmarks for performance validation.

**Run Command**:
```bash
# Run all tests
go test ./...

# Run benchmarks
go test -bench=. ./...
```
