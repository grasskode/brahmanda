// Command srishti is the runner: it executes one worker invocation
// declared by a pipeline spec. brahma (the orchestrator) spawns one
// srishti per pool slot per tick; it is never run by hand outside
// debugging. Flag contract:
//
//	srishti -pipeline NAME -worker-id ID -state-root PATH -command STR -timeout DUR [-worker-index N]
//
// Exit codes: 0 = worker succeeded, 124 = worker timed out, 1 = worker
// failed (non-zero exit or never claimed a task), 2 = invalid flags.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/grasskode/brahmanda/internal/journal"
	"github.com/grasskode/brahmanda/internal/runner"
)

func main() {
	var (
		pipeline    = flag.String("pipeline", "", "pipeline name (required)")
		workerID    = flag.String("worker-id", "", "unique invocation ID (required)")
		stateRoot   = flag.String("state-root", "", "orchestrator state dir (required)")
		command     = flag.String("command", "", "shell command to run as the worker (required)")
		timeout     = flag.Duration("timeout", 0, "max worker runtime, e.g. 1h (required)")
		workerIndex = flag.Int("worker-index", 0, "pool slot index (0..pool_size-1)")
	)
	flag.Parse()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	result, err := runner.Run(ctx, runner.Config{
		Pipeline:    *pipeline,
		WorkerID:    *workerID,
		StateRoot:   *stateRoot,
		Command:     *command,
		Timeout:     *timeout,
		WorkerIndex: *workerIndex,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "srishti: %v\n", err)
		os.Exit(2)
	}

	fmt.Fprintf(os.Stderr, "srishti: outcome=%s task=%s exit=%d note=%q\n",
		result.Outcome, result.TaskID, result.ExitCode, result.Note)

	switch result.Outcome {
	case journal.OutcomeSucceeded:
		os.Exit(0)
	case journal.OutcomeTimedOut:
		os.Exit(124)
	default:
		os.Exit(1)
	}
}
