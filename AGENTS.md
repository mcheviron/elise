# Repository Guidelines

## Overview

- Minimal Kafka-like TCP broker used for protocol exploration; no persistent state yet.
- Runtime server code resides in `internal/broker`, protocol helpers under `internal/protocol`, and metadata ingestion in `internal/metadata`.

## Development Commands

- Always prefer `just` targets: `just build`, `just run`, `just fmt`, `just test`, `just check`.
- Install tooling once via `just install` (Go 1.25.3).
- After making code changes, run `just check` before delivering output.

## Coding Style

- Rely on `just fmt` formatting and run it after making code changes.
- Keep log messages lowercase and concise.

## Testing

- Favor end-to-end validation via the CLI utilities under `cmd/` (e.g., DescribeTopicPartitions testers) or simple shell probes (`xxd`, `nc`, etc.).
- Add unit tests alongside code (`*_test.go`) when logic is complex.

## ExecPlans

When writing complex features or significant refactors, use an ExecPlan (as described in .agent/PLANS.md) from design to implementation.
