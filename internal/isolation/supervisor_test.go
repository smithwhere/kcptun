package isolation

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestIsolatedWorkerProcess(t *testing.T) {
	mode := os.Getenv("KCPTUN_ISOLATION_TEST_MODE")
	if mode == "" {
		return
	}
	if path := os.Getenv("KCPTUN_ISOLATION_TEST_READY"); path != "" {
		if err := os.WriteFile(path, []byte("ready"), 0600); err != nil {
			os.Exit(2)
		}
	}
	switch mode {
	case "block":
		for {
			time.Sleep(time.Hour)
		}
	case "exit":
		os.Exit(0)
	default:
		os.Exit(2)
	}
}

func workerCommand(mode, ready string) *exec.Cmd {
	cmd := exec.Command(os.Args[0], "-test.run=^TestIsolatedWorkerProcess$")
	cmd.Env = append(os.Environ(), "KCPTUN_ISOLATION_TEST_MODE="+mode, "KCPTUN_ISOLATION_TEST_READY="+ready)
	return cmd
}

func waitForFile(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("worker did not become ready: %s", path)
}

func TestBlockedWorkersAreReapedAndCapacityRecovers(t *testing.T) {
	const count = 8
	s := NewSupervisor(count)
	dir := t.TempDir()
	contexts := make([]context.CancelFunc, count)
	errs := make(chan error, count)
	var readyPaths []string
	for i := 0; i < count; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		contexts[i] = cancel
		path := filepath.Join(dir, fmt.Sprint(i))
		readyPaths = append(readyPaths, path)
		go func() { errs <- s.Run(ctx, workerCommand("block", path)) }()
	}
	t.Cleanup(func() {
		for _, cancel := range contexts {
			cancel()
		}
	})
	for _, p := range readyPaths {
		waitForFile(t, p)
	}
	for _, cancel := range contexts {
		cancel()
	}
	for i := 0; i < count; i++ {
		select {
		case err := <-errs:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("worker error: %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("canceled worker did not return")
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := s.Run(ctx, workerCommand("exit", "")); err != nil {
		t.Fatalf("capacity did not recover: %v", err)
	}
}

func TestCancelOneWorkerKeepsAnotherAlive(t *testing.T) {
	s := NewSupervisor(2)
	dir := t.TempDir()
	ctxA, cancelA := context.WithCancel(context.Background())
	ctxB, cancelB := context.WithCancel(context.Background())
	defer cancelA()
	defer cancelB()
	var wg sync.WaitGroup
	wg.Add(2)
	doneA, doneB := make(chan error, 1), make(chan error, 1)
	go func() { defer wg.Done(); doneA <- s.Run(ctxA, workerCommand("block", filepath.Join(dir, "a"))) }()
	go func() { defer wg.Done(); doneB <- s.Run(ctxB, workerCommand("block", filepath.Join(dir, "b"))) }()
	defer func() { cancelA(); cancelB(); wg.Wait() }()
	waitForFile(t, filepath.Join(dir, "a"))
	waitForFile(t, filepath.Join(dir, "b"))
	cancelA()
	select {
	case err := <-doneA:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("first worker did not stop")
	}
	select {
	case err := <-doneB:
		t.Fatalf("second worker stopped: %v", err)
	default:
	}
	cancelB()
	select {
	case <-doneB:
	case <-time.After(10 * time.Second):
		t.Fatal("second worker did not stop")
	}
}

func TestStartFailureReleasesCapacity(t *testing.T) {
	s := NewSupervisor(1)
	if err := s.Run(context.Background(), exec.Command(filepath.Join(t.TempDir(), "missing-worker"))); err == nil {
		t.Fatal("expected start failure")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := s.Run(ctx, workerCommand("exit", "")); err != nil {
		t.Fatalf("capacity not recovered: %v", err)
	}
}

func TestCanceledWaitDoesNotStartWorker(t *testing.T) {
	s := NewSupervisor(1)
	ctx1, cancel1 := context.WithCancel(context.Background())
	done := make(chan error, 1)
	path := filepath.Join(t.TempDir(), "active")
	go func() { done <- s.Run(ctx1, workerCommand("block", path)) }()
	t.Cleanup(func() { cancel1(); <-done })
	waitForFile(t, path)
	ctx2, cancel2 := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel2()
	extra := filepath.Join(t.TempDir(), "unexpected")
	if err := s.Run(ctx2, workerCommand("exit", extra)); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v", err)
	}
	if _, err := os.Stat(extra); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("waiting worker started: %v", err)
	}
}
