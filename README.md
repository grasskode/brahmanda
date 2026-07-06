# brahmanda

A small, config-driven pipeline orchestrator. One YAML file describes a set of
pipelines; each pipeline keeps a fixed-size pool of workers full by spawning a
short-lived runner subprocess on every tick. The orchestrator never runs worker
code itself — it spawns runners, tracks counts, and records every outcome to a
journal that the monitor reads.

*brahmanda* (the cosmic egg that contains the universe) is the suite; it ships
three binaries, each named after the Hindu deity whose role it plays:

| Binary    | Deity                                   | Role                                                                 |
| --------- | --------------------------------------- | -------------------------------------------------------------------- |
| `brahma`  | Brahma, the creator                     | the **orchestrator** daemon — creates and refills every worker pool  |
| `srishti` | *sṛṣṭi*, the act of creation            | the **runner** — manifests (executes) one worker, then dies          |
| `chitra`  | Chitragupta, keeper of the karma ledger | the **monitor** — reads the journal and reports what every worker did |

Conceptually they are just **orchestrator**, **runner**, and **monitor** — the
names are flavour, and the code keeps the plain-English roles throughout (the
runner logic lives in `internal/runner`, etc.).

### Concepts

A handful of terms recur throughout; they mean:

- **pipeline** — a named stage declared in `config.yaml`, with its own command,
  pool size, tick interval, and timeout.
- **pool** — the set of workers a pipeline keeps running at once, bounded by
  `pool_size`.
- **tick** — one timer fire (`tick_interval`) at which the orchestrator tops the
  pool back up to `pool_size` if it has room ("lazy fill").
- **worker** — one execution of a pipeline's command, run by a runner.
- **task** — a unit of work a worker claims, identified by `task_id`; the monitor
  folds all events sharing a `task_id` into one row.

## How it works

```
                config.yaml
                     │
                     ▼
              ┌──────────────┐         one per pool slot, per tick
              │    brahma     │ ── spawns ──▶  srishti (runner) ──▶ sh -c "<command>"   (your worker)
              │ (orchestrator)│                  │
              └──────────────┘                  │ parses worker stdout (NDJSON)
                     │                          ▼
                     │ writes              ┌──────────┐
                     └────────────────────▶│  journal  │◀── reads ── chitra (monitor)
                                           └──────────┘
```

- **brahma**: The orchestrator loads a `config.yaml`. For each pipeline it fires
  once immediately, then once per `tick_interval`. Each fire spawns one `srishti`
  **if** the number of running workers is below `pool_size` (otherwise the tick
  is a no-op — this is "lazy fill"). On `SIGINT`/`SIGTERM` it stops spawning and
  drains in-flight runners before exiting. A single-instance `flock` on
  `<state_dir>/brahma.lock` keeps two daemons from racing.

- **srishti**: The runner runs your worker command under `sh -c`, so any shell
  construct (pipes, `&&`, env interpolation) works. It enforces `step_timeout`
  by sending `SIGTERM` to the worker's whole process group, then `SIGKILL` 5s
  later — so grandchildren die with it. It parses the worker's stdout as NDJSON
  journal events, and if the worker exits without writing a terminal event, srishti
  writes one derived from the exit status (so a silent failure is never invisible).

- **chitra**: The monitor is a read-only projection of the journal and state dir. It
  needs no arguments: it reads `<state_dir>/runtime.yaml` (written by brahma at startup)
  to auto-discover everything. One-shot text by default; `--watch <interval>`
  opens a bubbletea TUI with toggleable panels and a worker-log browser.

## The worker contract

A worker is **any executable** named by a pipeline's `command`. It talks to
the orchestrator over **stdout**, one JSON object per line (NDJSON):

```json
{"task_id": "CHG-42", "phase": "build", "outcome": "started"}
{"task_id": "CHG-42", "phase": "build", "outcome": "succeeded", "note": "artifact built"}
```

| Field     | Required | Default            | Meaning                                                      |
| --------- | -------- | ------------------ | ------------------------------------------------------------ |
| `task_id` | yes      | —                  | the unit of work; what you query against in the monitor      |
| `phase`   | no       | the pipeline name  | which step this event belongs to                             |
| `outcome` | no       | `started`          | one of `started`, `succeeded`, `failed`, `timed_out`, `dead` |
| `note`    | no       | —                  | free-text detail (failure reason, "PR opened", …)            |

- **stderr** is captured to the worker's log file but never parsed as events.
- A worker that exits **0 without ever claiming a task** (no event emitted) is
  treated as a legitimate idle no-op and journals nothing — ideal for "poll for
  work, find none, exit" loops.
- A worker that exits **non-zero without claiming a task** gets a synthetic
  journal event carrying its stderr tail, surfaced in the monitor's *silent failures*
  panel.

The runner injects four environment variables (plus the operator's own env, which
propagates through):

| Variable             | Value                                          |
| -------------------- | ---------------------------------------------- |
| `AGENT_PIPELINE`     | the pipeline name                              |
| `AGENT_WORKER_ID`    | unique 8-char id for this invocation           |
| `AGENT_STATE_ROOT`   | the orchestrator state dir                     |
| `AGENT_WORKER_INDEX` | pool slot `0..pool_size-1` (for sharding)      |
| `AGENT_AUTONOMOUS`   | `1` — signals headless mode to interactive workers (e.g. Claude Code skills); harmless to ignore |

### Phase chains

The monitor folds events by `task_id`. When the **same task id flows through several
pipelines** — each pipeline emitting events whose `phase` equals the pipeline
name — the monitor renders the chain across stages:

```
CHG-1a2b3c4d   build ✓ → test ✓ → deploy ✓
CHG-5e6f7a8b   build ✓ → test ✗
```

That is the heart of the model: a multi-stage pipeline is just several
single-stage pipelines that hand work to each other, and chitra reconstructs the
journey from the journal. (Fine-grained sub-events whose `phase` differs from the
pipeline name are treated as noise and kept out of the chain.)

## Requirements

- **Go 1.24+** to build.
- **Linux or macOS.** The orchestrator relies on POSIX process groups, signals,
  and `flock` (single-instance lock, timeout kills, orphan reaping), so it does
  not build or run on Windows. WSL works.

## Quick start

```sh
make build                              # builds brahma, srishti, chitra into ./bin
./bin/brahma examples/simple/config.yaml
./bin/chitra --watch 2s                 # in another terminal
```

`make install` copies all three to `~/.local/bin`. With the module path matching
the repo you can also:

```sh
go install github.com/grasskode/brahmanda/cmd/brahma@latest
go install github.com/grasskode/brahmanda/cmd/srishti@latest
go install github.com/grasskode/brahmanda/cmd/chitra@latest
```

The orchestrator finds the runner via `$SRISHTI_BIN`, then a sibling next to the `brahma`
binary, then `srishti` on `$PATH`.

## Examples

Two runnable examples live under [`examples/`](examples), both writing to a
throwaway `~/.local/state/brahmanda-demo` state dir:

- **[`examples/simple`](examples/simple)** — fake jobs. Two independent pipelines
  run a worker that picks a random task and succeeds ~70% of the time. Shows pool
  sizing, lazy fill, and how outcomes roll up per pipeline.

  ```sh
  ./bin/brahma examples/simple/config.yaml
  ```

- **[`examples/pipeline`](examples/pipeline)** — a real-shaped `build → test →
  deploy` delivery pipeline. One script behaves as three stages (selected by
  `$AGENT_PIPELINE`), passing work between them through a shared queue. It
  demonstrates everything at once: **parallelism** (three stages concurrent,
  `test` runs a pool of 2), **conditional advancement** (changes that fail
  `test` are dead-lettered and never reach `deploy`), and **phase chains** (one
  change id tracked across all three stages).

  ```sh
  ./bin/brahma examples/pipeline/config.yaml
  ./bin/chitra --watch 2s
  ```

  After a few ticks, chitra's TASKS panel looks like:

  ```
  TASKS (all time, 6 tasks)
    CHG-0d2e9797  deploy done  13s ago
      build ✓ → test ✓ → deploy ✓
        ↳ deployed to prod
    CHG-218318c0  test failed  9s ago
      build ✓ → test ✗
        ↳ 3 tests failed
    CHG-54b6852f  build failed  5s ago
      build ✗
        ↳ compile error
  ```

## Config

```yaml
state_dir: ~/.local/state/brahmanda   # journal + worker state live here
log_file: ""                       # brahma's own log; empty = stderr
env_file: ./worker.env             # optional; env exported to every worker
pipelines:
  - name: build                    # kebab-case, unique
    pool_size: 1                   # max concurrent workers (default 1)
    tick_interval: 8s              # delay between pool-fill checks (default 5m)
    step_timeout: 1m               # per-worker kill deadline (default 1h)
    command: ./examples/pipeline/stage.sh
```

### Worker environment

Workers inherit the orchestrator's own process environment, so the simplest way to pass
something to every worker is to export it before starting `brahma`. To keep it
centralized in the config instead, point `env_file` at a dotenv-style file:

```ini
# worker.env — one KEY=value per line; '#' comments and blank lines ignored,
# a leading `export ` is tolerated, surrounding quotes are stripped.
LOG_LEVEL=debug
GITHUB_TOKEN=ghp_xxx
REGION="eu-west-1"
```

- A relative `env_file` path resolves against the config file's directory.
- These values **override** anything the orchestrator inherited from its own environment.
- `AGENT_*` names are reserved (the runner injects them) and rejected.
- It is not a shell: no `$VAR` interpolation or command substitution.

Keep the env file out of version control (it usually holds secrets) and
`chmod 600` it. The orchestrator reads it at startup, so restart `brahma` after editing it.

## State layout

Everything lives under `state_dir`:

```
brahma.lock                          single-instance flock (+ pid)
runtime.yaml                         configured pipelines, for chitra's auto-discovery
accounts.yaml                        operator identity panel (optional)
chitra.yaml                          chitra's saved panel preferences
journal/<pool>/<worker_id>.jsonl     append-only per-worker event log
workers/<pool>/<worker_id>.pid       liveness for the RUNNING count
workers/<pool>/<worker_id>.log       worker stdout+stderr
agent-errors/claude                  optional backoff marker
```

On restart, the orchestrator reaps orphans: a worker that logged `started` but no terminal
event, and whose process is gone, gets a `dead` event written on its behalf — so
it reads as dead in the monitor instead of "in progress" forever.

`dist/cleanup.sh` prunes old journal files; `dist/logrotate.d/brahma` rotates
orchestrator logs. See the comments in each for installation.

## Layout

```
cmd/brahma/            orchestrator daemon entry
cmd/srishti/           per-worker runner subprocess entry
cmd/chitra/            read-only monitor
internal/pipelinespec/ config.yaml loader + validation
internal/runner/       runner core (exec, timeout, journal routing)
internal/agentui/       monitor snapshot collection + rendering (text + TUI)
internal/journal/       per-worker NDJSON event log
internal/lock/          single-instance flock
internal/state/         state dir helpers
internal/tokens/        Claude token accounting (optional, via ccusage)
```

## Orchestrating Claude Code agents

brahma is worker-agnostic, but it grew up driving fleets of headless
[Claude Code](https://claude.com/claude-code) agents, and a few features cater to
that: the `AGENT_AUTONOMOUS` signal, the optional `agent-errors/claude` backoff
marker, and chitra's **token burn** panel, which attributes Claude API
input/output/cache tokens (and, when [`ccusage`](https://github.com/ryoppippi/ccusage)
is on `$PATH`, dollar cost) per pipeline. Point chitra at the agents' worktrees
with `--worktrees-root` (or `$WORKTREES_ROOT`) to enable it. None of this is
required for ordinary shell workers.

The backoff marker is a convention, not orchestrator machinery: a worker that
hits a Claude rate/quota limit writes `<state_dir>/agent-errors/claude`, and
Claude-using workers check for a recent marker on startup and skip their tick
until it clears — so one worker hitting a limit eases every worker off without
any central coordination. Ordinary workers never write it and are unaffected.

## License

[MIT](LICENSE) — free to use and modify, provided the copyright and license
notice are preserved.
