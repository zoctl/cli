//go:build linux || darwin

package transfer

import (
	"errors"
	"os"
	"syscall"
)

// Lock takes the workspace for one writer, a sync or a cutover, and returns
// what releases it. A second taker gets ErrBusy at once, whether it is this
// process or another. The lock is flock(2) on a file in the workspace, so
// the kernel drops it when the holder dies: nothing is left to clean up
// after a crash.
func (w *Workspace) Lock() (func(), error) {
	f, err := w.root.OpenFile(lockName, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, ErrWorkspace
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, ErrBusy
		}
		return nil, ErrWorkspace
	}
	return func() { f.Close() }, nil
}
