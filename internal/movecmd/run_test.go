package movecmd

import (
	"io"
	"strings"
)

// run is the command line's dispatch for these commands, as a test calls it.
func run(args []string, out io.Writer) error {
	in := strings.NewReader("")
	switch {
	case len(args) > 1 && args[0] == "runner" && args[1] == "pair":
		return PairRunner(args[2:], out, in)
	case len(args) > 0 && args[0] == "sync":
		return Sync(args[1:], out, in)
	case len(args) > 0 && args[0] == "cutover":
		return Cutover(args[1:], out, in)
	}
	return ErrConfig
}
