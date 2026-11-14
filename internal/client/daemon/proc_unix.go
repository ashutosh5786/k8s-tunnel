//go:build !windows

package daemon

import (
	"errors"
	"os"
	"os/exec"
	"syscall"
)

// Alive reports whether a process with this id is running.
func Alive(pid int) bool {
	if pid <= 0 {
		return false
	}
	p, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	// Signal 0 checks for existence without disturbing the process. A
	// permission error still means it exists.
	err = p.Signal(syscall.Signal(0))
	return err == nil || errors.Is(err, syscall.EPERM)
}

// Detach makes cmd survive the terminal that started it by giving it a
// session of its own, so closing the terminal does not hang it up.
func Detach(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
}
