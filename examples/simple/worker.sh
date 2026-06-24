#!/bin/sh
# worker.sh — the "fake jobs" demo worker.
#
# Picks a task_id at random from a fixed pool, simulates a little work
# (sleep 1), then succeeds ~70% of the time and fails ~30% of the time.
# It exercises brahma + srishti + chitra end-to-end without touching any
# real system — handy for watching the journal fill up and seeing how
# failures surface in the monitor.
#
# A worker talks to bramha purely over stdout: each line is one NDJSON
# journal event. Only "task_id" is required; srishti fills in the rest.
# Everything written to stderr is captured to the worker log but is NOT
# parsed as an event.
#
# Env injected by srishti (all four are always present):
#   AGENT_PIPELINE     — pipeline name from the spec
#   AGENT_WORKER_ID    — unique invocation ID
#   AGENT_STATE_ROOT   — bramha state dir
#   AGENT_WORKER_INDEX — pool slot (0..pool_size-1)

set -eu

# Random integer in [1..max].
randint() {
	awk -v max="$1" 'BEGIN { srand(); print int(rand() * max) + 1 }'
}

tasks="DEMO-1 DEMO-2 DEMO-3 DEMO-4 DEMO-5"
n=$(echo "$tasks" | wc -w)
task_id=$(echo "$tasks" | cut -d' ' -f"$(randint "$n")")

# Claim the task: a "started" event makes it show up in chitra as in-progress.
printf '{"task_id":"%s","outcome":"started"}\n' "$task_id"
echo "[${AGENT_PIPELINE:-?}/${AGENT_WORKER_ID:-?} idx=${AGENT_WORKER_INDEX:-?}] picked $task_id" >&2

sleep 1

roll=$(randint 100)
if [ "$roll" -le 30 ]; then
	printf '{"task_id":"%s","outcome":"failed","note":"random failure (roll=%s)"}\n' "$task_id" "$roll"
	exit 1
fi
printf '{"task_id":"%s","outcome":"succeeded","note":"random success (roll=%s)"}\n' "$task_id" "$roll"
