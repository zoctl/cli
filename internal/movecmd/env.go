// Package movecmd is zoctl's commands that move a customer's data and DNS:
// sync, cutover and runner pair. They run where the customer runs them, with
// both sides' keys in the environment, and print counts, never a bucket,
// key, table or account.
package movecmd

import (
	"errors"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"
)

// ErrConfig means the command line or the move file was refused. The command
// has printed why, where it could say without naming a secret.
var ErrConfig = errors.New("invalid configuration")

var errConfig = ErrConfig

// takeEnv reads a variable and takes it out of the environment, so nothing
// this process starts inherits a key.
func takeEnv(name string) string {
	v := os.Getenv(name)
	_ = os.Unsetenv(name)
	return v
}

// Sync is zoctl sync.
func Sync(args []string, out io.Writer, in io.Reader) error { return syncData(args, out, in) }

// Cutover is zoctl cutover.
func Cutover(args []string, out io.Writer, in io.Reader) error { return cutover(args, out, in) }

// PairRunner is zoctl runner pair.
func PairRunner(args []string, out io.Writer, in io.Reader) error { return pairRunner(args, out, in) }

// maxApprovalBytes bounds the approval read from the terminal.
const maxApprovalBytes = 128

// readApproval reads one bounded line. It never echoes what it read.
//
// It reads one line, a byte at a time so nothing past it is taken: a terminal
// sends Enter, not end of file, and reading to the end waited for Ctrl-D.
func readApproval(in io.Reader) (string, error) {
	var line []byte
	one := make([]byte, 1)
	for {
		n, err := in.Read(one)
		if n == 1 {
			if one[0] == '\n' {
				break
			}
			// A line longer than any approval isn't cut to fit: the
			// digest padded out to the limit would otherwise match.
			if len(line) == maxApprovalBytes {
				return "", errConfig
			}
			line = append(line, one[0])
		}
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return "", errConfig
		}
	}
	return strings.TrimSpace(string(line)), nil
}

// interrupts is the operator's Ctrl-C. A test replaces it rather than
// signalling the test binary.
var interrupts = func() (<-chan os.Signal, func()) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	return c, func() { signal.Stop(c) }
}
