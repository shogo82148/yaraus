package yaraus

import (
	"context"
	"fmt"
	"os"
	"os/exec"
)

// Runner is a runner.
type Runner interface {
	Run(ctx context.Context, workerID uint) error
}

// RunnerFunc is a runner for calling func.
type RunnerFunc func(ctx context.Context, workerID uint) error

// Run calls func.
func (r RunnerFunc) Run(ctx context.Context, id uint) error {
	return r(ctx, id)
}

// CommandRunner is a runner of a command.
type CommandRunner struct {
	replacement string
	cmd         []string
}

// NewCommandRunner returns new runner.
func NewCommandRunner(replacement string, cmd []string) Runner {
	return &CommandRunner{
		replacement: replacement,
		cmd:         cmd,
	}
}

// Run executes the command.
func (r *CommandRunner) Run(ctx context.Context, id uint) error {
	args := make([]string, len(r.cmd)-1)
	for i, arg := range r.cmd[1:] {
		if arg == r.replacement {
			args[i] = fmt.Sprintf("%d", id)
		} else {
			args[i] = arg
		}
	}
	cmd := exec.CommandContext(ctx, r.cmd[0], args...)
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	// TODO: forward signals to child
	// TODO: get exit code

	return cmd.Run()
}
