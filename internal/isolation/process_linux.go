//go:build linux

package isolation

import (
	"fmt"
	"os/exec"
	"syscall"
)

func configureWorker(cmd *exec.Cmd) error {
	if cmd.SysProcAttr == nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{}
	}
	if cmd.SysProcAttr.Pgid != 0 || cmd.SysProcAttr.Setsid || cmd.SysProcAttr.Foreground {
		return fmt.Errorf("isolated worker cannot use a preassigned process group or session")
	}
	cmd.SysProcAttr.Setpgid = true
	return nil
}

// Killing the group also terminates ordinary iptables descendants. The caller
// must still reconcile firewall rules after the process has exited.
func terminateWorker(cmd *exec.Cmd) error {
	return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
}
