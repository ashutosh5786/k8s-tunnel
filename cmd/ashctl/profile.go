package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/ashutosh5786/k8s-tunnel/internal/client/daemon"
	"github.com/ashutosh5786/k8s-tunnel/internal/client/profile"
)

// runGenerate writes a commented starting ashctl.yaml.
func runGenerate(args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("ashctl generate", flag.ContinueOnError)
	fs.SetOutput(stderr)
	out := fs.String("o", profile.DefaultName, "file to write")
	force := fs.Bool("force", false, "replace the file if it already exists")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if fs.NArg() > 0 {
		return fmt.Errorf("unexpected argument %q", fs.Arg(0))
	}

	if err := writeNewFile(*out, profile.Template, *force); err != nil {
		return err
	}
	fmt.Fprintf(stdout, "Wrote %s. Edit it, then run \"ashctl apply\".\n", *out)
	return nil
}

// writeNewFile writes body to path. It refuses to replace an existing file
// unless force is set.
func writeNewFile(path, body string, force bool) error {
	flags := os.O_WRONLY | os.O_CREATE | os.O_EXCL
	if force {
		flags = os.O_WRONLY | os.O_CREATE | os.O_TRUNC
	}
	// O_EXCL makes the "already exists" check and the creation one step,
	// so a file that appears in between is never overwritten.
	f, err := os.OpenFile(path, flags, 0o644)
	if errors.Is(err, os.ErrExist) {
		return fmt.Errorf("%s already exists; edit it, or use --force to replace it", path)
	}
	if err != nil {
		return err
	}
	if _, err := io.WriteString(f, body); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

// runApply makes the connection match an ashctl.yaml: it connects if
// there is no connection, and otherwise changes what the running one
// exposes.
func runApply(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("ashctl apply", flag.ContinueOnError)
	fs.SetOutput(stderr)
	path := fs.String("f", profile.DefaultName, "file to apply")
	prune := fs.Bool("prune", false, "also unexpose everything the file does not list")
	user := fs.String("user", defaultUser(), "user name that replaces ${USER} in the file")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if fs.NArg() > 0 {
		return fmt.Errorf("unexpected argument %q", fs.Arg(0))
	}
	file, err := profile.Load(*path, *user)
	if err != nil {
		return err
	}
	dir, err := daemon.DefaultDir()
	if err != nil {
		return err
	}

	state, running := dir.Running()
	if !running {
		// Nothing is connected: start a connection that has everything
		// from the file in place from the beginning.
		connectArgs := []string{}
		add := func(flag, value string) {
			if value != "" {
				connectArgs = append(connectArgs, flag, value)
			}
		}
		add("--context", file.Context)
		add("--namespace", file.Namespace)
		add("--home-namespace", file.HomeNamespace)
		add("--hosts-namespaces", strings.Join(file.HostsNamespaces, ","))
		add("--user", *user)
		for _, spec := range file.ExposeSpecs() {
			connectArgs = append(connectArgs, "--expose", spec)
		}
		for _, spec := range file.ForwardSpecs() {
			connectArgs = append(connectArgs, "--forward", spec)
		}
		return runConnect(ctx, connectArgs, stdout, stderr)
	}

	// A connection is running. It can change what it exposes, but which
	// cluster it is on and its forwards were fixed when it started.
	if file.Context != "" && state.Context != "" && file.Context != state.Context {
		return fmt.Errorf("%s is for context %q, but you are connected to %q; run \"ashctl disconnect\" first",
			*path, file.Context, state.Context)
	}
	if state.Control == "" {
		return errors.New("the running connection was started by an older ashctl: disconnect and apply again")
	}
	controlArgs := file.ExposeSpecs()
	if *prune {
		controlArgs = append([]string{"--prune"}, controlArgs...)
	}
	out, err := daemon.Call(state.Control, state.Token, "apply", controlArgs, daemonCallTimeout)
	io.WriteString(stdout, out)
	if err != nil {
		return err
	}
	if out == "" {
		fmt.Fprintf(stdout, "Nothing to expose in %s.\n", *path)
	}
	if len(file.Forward) > 0 || file.HomeNamespace != "" || len(file.HostsNamespaces) > 0 {
		fmt.Fprintln(stdout, "note: forward, homeNamespace and hostsNamespaces only take effect when connecting. To apply changes to them, run \"ashctl disconnect\" and apply again.")
	}
	return nil
}
