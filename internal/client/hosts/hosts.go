// Package hosts manages the hosts-file entries that make cluster service
// names resolve to local loopback addresses.
//
// Every line this package writes ends with a marker comment. Updating and
// removing work by dropping the marked lines, so nothing else in the file
// is ever touched and a crashed run leaves nothing that cannot be cleaned.
package hosts

import (
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
)

// marker ends every managed line.
const marker = "# k8s-tunnel"

// Entry maps one address to the names that should resolve to it.
type Entry struct {
	IP    string
	Names []string
}

// DefaultPath returns the system hosts file.
func DefaultPath() string {
	if runtime.GOOS == "windows" {
		root := os.Getenv("SystemRoot")
		if root == "" {
			root = `C:\Windows`
		}
		return filepath.Join(root, "System32", "drivers", "etc", "hosts")
	}
	return "/etc/hosts"
}

// nameRE accepts lowercase DNS names only. Names come from the cluster,
// so anything that could break out of a hosts line is refused.
var nameRE = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$`)

func validate(entries []Entry) error {
	for _, e := range entries {
		ip := net.ParseIP(e.IP)
		if ip == nil || !ip.IsLoopback() {
			return fmt.Errorf("hosts: refusing non-loopback address %q", e.IP)
		}
		if len(e.Names) == 0 {
			return fmt.Errorf("hosts: entry for %s has no names", e.IP)
		}
		for _, n := range e.Names {
			if len(n) > 253 || !nameRE.MatchString(n) {
				return fmt.Errorf("hosts: invalid host name %q", n)
			}
			if n == "localhost" {
				return errors.New(`hosts: refusing to remap "localhost"`)
			}
		}
	}
	return nil
}

func isManaged(line []byte) bool {
	return bytes.HasSuffix(bytes.TrimRight(line, " \t\r\n"), []byte(marker))
}

// Render returns existing with all managed lines replaced by entries.
// Every other byte of the file is kept as it was.
func Render(existing []byte, entries []Entry) ([]byte, error) {
	if err := validate(entries); err != nil {
		return nil, err
	}
	eol := "\n"
	if bytes.Contains(existing, []byte("\r\n")) {
		eol = "\r\n"
	}
	var out bytes.Buffer
	for _, line := range bytes.SplitAfter(existing, []byte("\n")) {
		if len(line) > 0 && !isManaged(line) {
			out.Write(line)
		}
	}
	if len(entries) == 0 {
		return out.Bytes(), nil
	}
	if out.Len() > 0 && !bytes.HasSuffix(out.Bytes(), []byte("\n")) {
		out.WriteString(eol)
	}
	for _, e := range entries {
		fmt.Fprintf(&out, "%s %s %s%s", e.IP, strings.Join(e.Names, " "), marker, eol)
	}
	return out.Bytes(), nil
}

// Update rewrites the managed lines of the hosts file at path. With no
// entries it removes them. The file is only written when it would change.
func Update(path string, entries []Entry) error {
	existing, err := os.ReadFile(path)
	if err != nil {
		return explain(path, err)
	}
	updated, err := Render(existing, entries)
	if err != nil {
		return err
	}
	if bytes.Equal(existing, updated) {
		return nil
	}
	mode := fs.FileMode(0o644)
	if info, err := os.Stat(path); err == nil {
		mode = info.Mode().Perm()
	}
	// Written in place: replacing the file by rename would drop its
	// permissions and is often blocked for the system hosts file.
	return explain(path, os.WriteFile(path, updated, mode))
}

// Remove deletes every managed line from the hosts file at path.
func Remove(path string) error {
	return Update(path, nil)
}

// CheckWritable reports whether the hosts file can be modified, without
// changing it. Calling it first turns a late failure into an early one.
func CheckWritable(path string) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		return explain(path, err)
	}
	return f.Close()
}

func explain(path string, err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, fs.ErrPermission) {
		how := "run the command with sudo"
		if runtime.GOOS == "windows" {
			how = "run the command from a terminal started with \"Run as administrator\""
		}
		return fmt.Errorf("cannot modify hosts file %s: %s: %w", path, how, err)
	}
	return fmt.Errorf("hosts file %s: %w", path, err)
}
