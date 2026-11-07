//go:build linux

package isolation

import (
	"os"
	"os/exec"
	"syscall"
	"testing"
)

func TestWorkerUsesDedicatedProcessGroup(t *testing.T) {
	cmd := exec.Command(os.Args[0], "-test.run=^TestIsolatedWorkerProcess$")
	if err := configureWorker(cmd); err != nil {
		t.Fatal(err)
	}
	if cmd.SysProcAttr == nil || !cmd.SysProcAttr.Setpgid || cmd.SysProcAttr.Pgid != 0 {
		t.Fatalf("worker process group not isolated: %+v", cmd.SysProcAttr)
	}
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true, Pgid: 1}
	if err := configureWorker(cmd); err == nil {
		t.Fatal("accepted externally assigned process group")
	}
}
