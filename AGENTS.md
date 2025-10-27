# Repository Guidelines

## Overview
- Minimal Kafka-like TCP broker used for protocol exploration; there is no persistence or broker state yet.
- Broker listens on `:9092` (see `main.go`) and logs connection lifecycle events.
- Protocol logic lives under `internal/protocol`, providing request/response parsing helpers reused across handlers.

## Current Capabilities
- Accepts ApiVersions requests versions 0-4 and returns the supported API list defined in `protocol.SupportedAPIs`.
- Parses DescribeTopicPartitions v0 requests using flexible headers; responses mark every topic as `UNKNOWN_TOPIC_OR_PARTITION` because topic metadata is not implemented.
- Unsupported API keys are logged and cause the connection to close; there is no further request routing.

## Project Structure & Module Organization
- `main.go`: minimal TCP loop that accepts clients, dispatches on API key, and calls response builders.
- `internal/protocol/api.go`: enumerates supported API keys/version ranges and error codes.
- `internal/protocol/request.go`: parses request headers, compact strings, tagged fields, and delegates to body parsers.
- `internal/protocol/describe_topic_partitions.go`: typed parser for DescribeTopicPartitions requests and cursors.
- `internal/protocol/response.go`, `binary_writers.go`, `binary_readers.go`: shared encoders/decoders for response bodies and primitive Kafka wire types.
- `justfile`: task runner with install/build/test/check jobs.
- Future packages live under `internal/` or `pkg/`; keep integration fixtures inside `testdata/`.

## Build, Test, and Development Commands
- Toolchain: Go 1.25.3 (from `go.mod`); run `just install` once to pull `goimports`, `air`, `deadcode`, and `staticcheck`.
- Always use the `just` targets; avoid running raw `go` commands directly.
- `just build`: compiles all Go packages to ensure the broker and any future packages build cleanly.
- `just run`: starts `air` for live reload; pass flags like ``just run -- --build.cmd "go build"`` if you tweak build steps.
- `just test`: runs `go test ./...` (there are currently no test packages; add them alongside new features).
- `just fmt`: rewrites code with `goimports`; run after edits to normalize imports and formatting.
- `just check`: executes formatting, vetting, static analysis, deadcode, and module tidy in one sweep.

## Coding Style & Naming Conventions
- Follow `goimports` formatting; never hand-edit import blocks.
- Stick to Go naming: exported identifiers use CamelCase, unexported use lowerCamelCase.
- Group broker logic by concern (e.g., `internal/network`, `internal/protocol`); expose only necessary types from `pkg/` if consumers need them.
- Keep log messages structured: lower-case verbs, no trailing punctuation.

## Testing Guidelines
- Use the standard library `testing` package; table-driven tests preferred.
- Place unit tests alongside code (`foo_test.go`); integration scenarios can live under `internal/...` with descriptive package names.
- Add benchmarks where latency matters (`func BenchmarkXxx(b *testing.B)`).
- For new features, ensure `just test` passes and add assertions covering error paths and handshake edge cases.

## Security & Configuration Tips
- Bind only to trusted interfaces in production by replacing `:9092` with an explicit address.
- Never commit `.env` or broker credentials; store secrets in your deployment platform.
- Validate incoming frames defensively; reject malformed requests before parsing payloads.
