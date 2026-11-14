//go:build windows

package daemon

import (
	"os/exec"
	"syscall"
)

const (
	processQueryLimitedInformation = 0x1000
	stillActive                    = 259

	detachedProcess       = 0x00000008
	createNewProcessGroup = 0x00000200
)

// Alive reports whether a process with this id is running.
func Alive(pid int) bool {
	if pid <= 0 {
		return false
	}
	h, err := syscall.OpenProcess(processQueryLimitedInformation, false, uint32(pid))
	if err != nil {
		return false
	}
	defer syscall.CloseHandle(h)
	var code uint32
	if err := syscall.GetExitCodeProcess(h, &code); err != nil {
		return false
	}
	return code == stillActive
}

// Detach makes cmd survive the terminal that started it: it gets no
// console of its own and is not in the terminal's process group, so
// closing the window or pressing Ctrl-C there does not reach it.
func Detach(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{
		HideWindow:    true,
		CreationFlags: detachedProcess | createNewProcessGroup,
	}
}
