# Repository Guidelines

## Overview

- Minimal Kafka-like TCP broker used for protocol exploration; no persistent state yet.
- Runtime server code resides in `internal/broker`, protocol helpers under `internal/protocol`, and metadata ingestion in `internal/metadata`.

## Development Commands

- Always prefer `just` targets: `just build`, `just run`, `just fmt`, `just test`, `just check`.
- Install tooling once via `just install`.
- After making code changes, run `just check` before delivering output.

## Coding Style

- Rely on `just fmt` formatting and run it after making code changes.
- Keep log messages lowercase and concise.

## Testing

- Favor end-to-end validation via the CLI utilities under `cmd/` (e.g., DescribeTopicPartitions testers) or simple shell probes (`xxd`, `nc`, etc.).
- Add unit tests alongside code (`*_test.go`) when logic is complex.
- For protocol smoke tests that need a live broker (ApiVersions, Fetch, future handlers), run `just run` in a second terminal or background the command locally (for example `just run >/tmp/elise_server.log 2>&1 &`) and record the PID so you can stop it later. After the log line `Broker listening on :9092` appears, exercise the feature from another shell using whatever helper makes sense (Python script, `nc`, CLI under `cmd/`). Capture the exact command and expected output in your notes or plan so future contributors understand how to validate the behavior. Always terminate the background `just run` process when you are done testing.

## ExecPlans

When writing complex features or significant refactors, use an ExecPlan (as described in .agent/PLANS.md) from design to implementation.
