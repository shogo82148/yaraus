package yaraus

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"syscall"
)

// TrapSignals are trap signals while CommandRunner.Run
var TrapSignals = []os.Signal{
	syscall.SIGHUP,
	syscall.SIGINT,
	syscall.SIGTERM,
	syscall.SIGQUIT,
}

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
	err := cmd.Start()
	if err != nil {
		return err
	}

	// forward signals to child
	go func() {
		signalCh := make(chan os.Signal, 1)
		signal.Notify(signalCh, TrapSignals...)
		for {
			select {
			case s := <-signalCh:
				cmd.Process.Signal(s)
			}
		}
	}()

	return cmd.Wait()
}
