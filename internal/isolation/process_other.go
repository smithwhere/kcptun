//go:build !linux

package isolation

import "os/exec"

func configureWorker(*exec.Cmd) error     { return nil }
func terminateWorker(cmd *exec.Cmd) error { return cmd.Process.Kill() }
