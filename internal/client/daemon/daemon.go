// Package daemon lets the client run in the background: it records which
// process holds the connection, where its log is, and carries the request
// to stop it.
//
// Everything is plain files in one directory, so the foreground command
// and the background process need no other channel between them and the
// same code works on every operating system.
package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"time"
)

// State describes the background process.
type State struct {
	PID     int       `json:"pid"`
	Context string    `json:"context,omitempty"`
	Session string    `json:"session"`
	Started time.Time `json:"started"`
	// Control is the address of the control channel, and Token the secret
	// that requests to it must carry. Empty when there is none.
	Control string `json:"control,omitempty"`
	Token   string `json:"token,omitempty"`
}

// Dir is the directory holding the background process's files.
type Dir string

// DefaultDir returns the per-user directory, or the one named by the
// ASHCTL_HOME environment variable.
func DefaultDir() (Dir, error) {
	if d := os.Getenv("ASHCTL_HOME"); d != "" {
		return Dir(d), nil
	}
	base, err := os.UserConfigDir()
	if err != nil {
		return "", err
	}
	return Dir(filepath.Join(base, "ashctl")), nil
}

func (d Dir) statePath() string { return filepath.Join(string(d), "state.json") }
func (d Dir) stopPath() string  { return filepath.Join(string(d), "stop") }

// LogPath is where the background process writes its output.
func (d Dir) LogPath() string { return filepath.Join(string(d), "daemon.log") }

// Write records the background process. It also withdraws any earlier
// stop request, which was meant for a previous process.
func (d Dir) Write(s State) error {
	if err := os.MkdirAll(string(d), 0o700); err != nil {
		return err
	}
	os.Remove(d.stopPath())
	body, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return err
	}
	// Written to a temporary file first so a reader never sees half of it.
	tmp := d.statePath() + ".tmp"
	if err := os.WriteFile(tmp, body, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, d.statePath())
}

// Read returns the recorded state. ok is false when nothing is recorded.
func (d Dir) Read() (s State, ok bool, err error) {
	body, err := os.ReadFile(d.statePath())
	if errors.Is(err, fs.ErrNotExist) {
		return State{}, false, nil
	}
	if err != nil {
		return State{}, false, err
	}
	if err := json.Unmarshal(body, &s); err != nil {
		return State{}, false, err
	}
	return s, true, nil
}

// Clear forgets the background process and any stop request.
func (d Dir) Clear() {
	os.Remove(d.statePath())
	os.Remove(d.stopPath())
}

// Running returns the recorded process if it is still alive. A record
// left behind by a process that died is cleared.
func (d Dir) Running() (State, bool) {
	s, ok, err := d.Read()
	if err != nil || !ok {
		return State{}, false
	}
	if !Alive(s.PID) {
		d.Clear()
		return State{}, false
	}
	return s, true
}

// RequestStop asks the background process to shut down cleanly.
func (d Dir) RequestStop() error {
	if err := os.MkdirAll(string(d), 0o700); err != nil {
		return err
	}
	return os.WriteFile(d.stopPath(), nil, 0o600)
}

// StopRequested reports whether a stop has been asked for.
func (d Dir) StopRequested() bool {
	_, err := os.Stat(d.stopPath())
	return err == nil
}

// stopPoll is how often the background process looks for a stop request.
const stopPoll = 300 * time.Millisecond

// WatchStop returns a context that is cancelled when a stop is requested
// or when ctx ends.
func (d Dir) WatchStop(ctx context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(ctx)
	go func() {
		t := time.NewTicker(stopPoll)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				if d.StopRequested() {
					cancel()
					return
				}
			}
		}
	}()
	return ctx, cancel
}

// WaitExit waits until the process is gone or the timeout passes, and
// reports whether it is gone.
func WaitExit(pid int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for Alive(pid) {
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(100 * time.Millisecond)
	}
	return true
}
