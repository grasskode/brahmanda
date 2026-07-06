#!/bin/sh
# stage.sh — one script, three behaviours, selected by $AGENT_PIPELINE.
#
# This is a self-contained mini software-delivery pipeline that shows off
# what brahma is actually for: a unit of work flowing through several
# stages, each stage its own pool, with conditional advancement between
# them. No real build system is involved — every step is a sleep + a coin
# flip — but the shape mirrors a real agent-workers pipeline (the
# investigate -> implement -> finalize flow), simplified.
#
# The three stages share a work queue under $AGENT_STATE_ROOT/demo-queue:
#
#   build  : mints a fresh change id (CHG-<worker>), "compiles" it, and on
#            success enqueues it for testing. The source of all work.
#   test   : claims one built change, "runs the tests". ~25% fail — those
#            are dead-lettered and DO NOT advance. Passing changes are
#            enqueued for deploy. This is the conditional step.
#   deploy : claims one tested change, "ships it". Terminal stage.
#
# Because the change id is carried in the queue filename, the SAME task_id
# flows through all three pools — so chitra renders one chain per change:
#
#   CHG-1a2b3c4d   build ✓ → test ✓ → deploy ✓
#   CHG-5e6f7a8b   build ✓ → test ✗            (dead-lettered at test)
#
# Parallelism comes for free: the three stages run concurrently, and any
# stage with pool_size > 1 (here: test) processes several changes at once.
# A stage that finds nothing to claim exits 0 without journaling, so idle
# polling stays invisible in the monitor.

set -eu

Q="${AGENT_STATE_ROOT:?AGENT_STATE_ROOT must be set by srishti}/demo-queue"
mkdir -p "$Q/built" "$Q/passed" "$Q/done" "$Q/failed" "$Q/claim"

# Random integer in [1..max].
randint() { awk -v max="$1" 'BEGIN { srand(); print int(rand() * max) + 1 }'; }

# emit TASK OUTCOME [NOTE] — one journal event for this stage. The phase
# is the pipeline name, which is what makes it a pipeline-level event that
# chitra folds into the per-task chain.
emit() {
	if [ -n "${3:-}" ]; then
		printf '{"task_id":"%s","phase":"%s","outcome":"%s","note":"%s"}\n' "$1" "$AGENT_PIPELINE" "$2" "$3"
	else
		printf '{"task_id":"%s","phase":"%s","outcome":"%s"}\n' "$1" "$AGENT_PIPELINE" "$2"
	fi
}

# claim_one SRC — atomically claim the first item in SRC by moving it into
# the per-run claim dir. mv on the same filesystem is atomic, so when two
# workers race for the same file only one wins; the loser's mv fails and
# it tries the next. Echoes the claimed id, or returns 1 when SRC is empty.
claim_one() {
	for f in "$1"/*; do
		[ -e "$f" ] || continue   # empty glob: nothing to claim
		id=$(basename "$f")
		if mv "$f" "$Q/claim/$id" 2>/dev/null; then
			echo "$id"
			return 0
		fi
	done
	return 1
}

case "$AGENT_PIPELINE" in
build)
	id="CHG-$AGENT_WORKER_ID"
	emit "$id" started
	echo "[build] compiling $id" >&2
	sleep 1
	if [ "$(randint 12)" -le 1 ]; then          # ~8% don't even compile
		emit "$id" failed "compile error"
		exit 1
	fi
	: > "$Q/built/$id"                           # enqueue for the test stage
	emit "$id" succeeded "artifact built"
	;;

test)
	id=$(claim_one "$Q/built") || exit 0         # nothing queued -> idle, no journal
	emit "$id" started
	echo "[test] running suite for $id" >&2
	sleep 1
	if [ "$(randint 4)" -le 1 ]; then            # ~25% fail -> dead-letter, does NOT advance
		mv "$Q/claim/$id" "$Q/failed/$id"
		emit "$id" failed "3 tests failed"
		exit 1
	fi
	mv "$Q/claim/$id" "$Q/passed/$id"            # conditional step: only green builds advance
	emit "$id" succeeded "all tests green"
	;;

deploy)
	id=$(claim_one "$Q/passed") || exit 0
	emit "$id" started
	echo "[deploy] shipping $id" >&2
	sleep 1
	if [ "$(randint 20)" -le 1 ]; then           # ~5% rollback
		mv "$Q/claim/$id" "$Q/failed/$id"
		emit "$id" failed "deploy rejected by canary"
		exit 1
	fi
	mv "$Q/claim/$id" "$Q/done/$id"
	emit "$id" succeeded "deployed to prod"
	;;

*)
	echo "stage.sh: unknown pipeline '$AGENT_PIPELINE'" >&2
	exit 2
	;;
esac
