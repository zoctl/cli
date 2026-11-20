//go:build !linux && !darwin

package transfer

// Lock refuses: a sync and a cutover run only where a lock holds, which is
// Linux and macOS.
func (w *Workspace) Lock() (func(), error) { return nil, ErrWorkspace }
