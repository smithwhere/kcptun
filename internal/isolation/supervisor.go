// Package isolation supervises operating-system worker processes.
package isolation

import (
	"context"
	"fmt"
	"os/exec"
)

// Supervisor limits simultaneously unreaped worker processes. Capacity is
// returned only after Wait confirms that a child has exited.
type Supervisor struct{ slots chan struct{} }

func NewSupervisor(capacity int) *Supervisor {
	if capacity <= 0 {
		panic("isolation: capacity must be positive")
	}
	return &Supervisor{slots: make(chan struct{}, capacity)}
}

// Run executes one worker. Cancellation terminates that worker without touching
// other workers. The slot remains reserved until the OS reports its exit.
func (s *Supervisor) Run(ctx context.Context, cmd *exec.Cmd) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case s.slots <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		<-s.slots
		return err
	}
	if err := configureWorker(cmd); err != nil {
		<-s.slots
		return fmt.Errorf("configure isolated worker: %w", err)
	}
	if err := cmd.Start(); err != nil {
		<-s.slots
		return fmt.Errorf("start isolated worker: %w", err)
	}
	done := make(chan error, 1)
	go func() { err := cmd.Wait(); <-s.slots; done <- err }()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		if err := terminateWorker(cmd); err != nil {
			select {
			case <-done:
				return ctx.Err()
			default:
				return fmt.Errorf("terminate isolated worker: %w", err)
			}
		}
		return ctx.Err()
	}
}
