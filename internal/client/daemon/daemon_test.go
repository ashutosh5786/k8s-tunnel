package daemon

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func TestStateRoundTrip(t *testing.T) {
	d := Dir(filepath.Join(t.TempDir(), "nested", "ashctl")) // must be created on demand
	if _, ok, err := d.Read(); ok || err != nil {
		t.Fatalf("empty dir: ok=%v err=%v", ok, err)
	}
	want := State{PID: 4242, Context: "dev", Session: "s1", Started: time.Now().UTC().Truncate(time.Second)}
	if err := d.Write(want); err != nil {
		t.Fatal(err)
	}
	got, ok, err := d.Read()
	if err != nil || !ok {
		t.Fatalf("ok=%v err=%v", ok, err)
	}
	if got != want {
		t.Errorf("got %+v, want %+v", got, want)
	}
	d.Clear()
	if _, ok, _ := d.Read(); ok {
		t.Error("state survived Clear")
	}
}

func TestCorruptStateIsAnErrorNotACrash(t *testing.T) {
	d := Dir(t.TempDir())
	os.WriteFile(filepath.Join(string(d), "state.json"), []byte("{not json"), 0o600)
	if _, ok, err := d.Read(); ok || err == nil {
		t.Errorf("ok=%v err=%v, want an error", ok, err)
	}
	if _, running := d.Running(); running {
		t.Error("corrupt state reported as running")
	}
}

func TestAlive(t *testing.T) {
	if !Alive(os.Getpid()) {
		t.Error("this process reported as not alive")
	}
	for _, pid := range []int{0, -1} {
		if Alive(pid) {
			t.Errorf("pid %d reported alive", pid)
		}
	}

	// A process that has exited must be reported dead.
	cmd := exec.Command(os.Args[0], "-test.run=^$")
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	pid := cmd.Process.Pid
	cmd.Wait()
	if !WaitExit(pid, 5*time.Second) {
		t.Errorf("exited process %d still reported alive", pid)
	}
}

func TestRunningClearsRecordOfDeadProcess(t *testing.T) {
	d := Dir(t.TempDir())
	cmd := exec.Command(os.Args[0], "-test.run=^$")
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	dead := cmd.Process.Pid
	cmd.Wait()
	WaitExit(dead, 5*time.Second)

	if err := d.Write(State{PID: dead, Session: "old"}); err != nil {
		t.Fatal(err)
	}
	if _, running := d.Running(); running {
		t.Fatal("dead process reported as running")
	}
	if _, ok, _ := d.Read(); ok {
		t.Error("record of a dead process was not cleared")
	}

	// A live process is reported, and its record is kept.
	if err := d.Write(State{PID: os.Getpid(), Context: "dev", Session: "live"}); err != nil {
		t.Fatal(err)
	}
	s, running := d.Running()
	if !running || s.Session != "live" || s.Context != "dev" {
		t.Errorf("running=%v state=%+v", running, s)
	}
}

func TestStopRequestCancelsWatcher(t *testing.T) {
	d := Dir(t.TempDir())
	ctx, cancel := d.WatchStop(context.Background())
	defer cancel()

	select {
	case <-ctx.Done():
		t.Fatal("cancelled before any stop request")
	case <-time.After(2 * stopPoll):
	}
	if d.StopRequested() {
		t.Fatal("stop reported before it was requested")
	}
	if err := d.RequestStop(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-ctx.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("watcher did not notice the stop request")
	}
}

func TestWatcherFollowsParentContext(t *testing.T) {
	d := Dir(t.TempDir())
	parent, stop := context.WithCancel(context.Background())
	ctx, cancel := d.WatchStop(parent)
	defer cancel()
	stop()
	select {
	case <-ctx.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("watcher outlived its parent context")
	}
}

// A stop request left over from the previous process must not stop the
// next one the moment it starts.
func TestWriteWithdrawsOldStopRequest(t *testing.T) {
	d := Dir(t.TempDir())
	if err := d.RequestStop(); err != nil {
		t.Fatal(err)
	}
	if err := d.Write(State{PID: os.Getpid(), Session: "new"}); err != nil {
		t.Fatal(err)
	}
	if d.StopRequested() {
		t.Error("stale stop request survived a new start")
	}
}

func TestDefaultDirHonoursOverride(t *testing.T) {
	want := t.TempDir()
	t.Setenv("ASHCTL_HOME", want)
	got, err := DefaultDir()
	if err != nil || string(got) != want {
		t.Errorf("got %q, err %v", got, err)
	}
	t.Setenv("ASHCTL_HOME", "")
	got, err = DefaultDir()
	if err != nil || filepath.Base(string(got)) != "ashctl" {
		t.Errorf("default = %q, err %v", got, err)
	}
}

func TestWaitExitTimesOutForLiveProcess(t *testing.T) {
	start := time.Now()
	if WaitExit(os.Getpid(), 300*time.Millisecond) {
		t.Error("reported this process as exited")
	}
	if took := time.Since(start); took > 3*time.Second {
		t.Errorf("took %v", took)
	}
}
