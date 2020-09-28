package file

import (
	"context"
	"io/ioutil"
	"os"
	"path/filepath"
)

// Persist implements the mast.Persist interface for storing and loading
// nodes from files.
type Persist struct {
	basepath string
}

// Load loads the bytes persisted in the named file.
func (p Persist) Load(ctx context.Context, name string) ([]byte, error) {
	return ioutil.ReadFile(filepath.Join(p.basepath, name))
}

// Store persists the given bytes in a file of the given name, if it
// doesn't exist already.
func (p Persist) Store(ctx context.Context, name string, bytes []byte) error {
	path := filepath.Join(p.basepath, name)
	_, err := os.Stat(path)
	if os.IsNotExist(err) {
		return ioutil.WriteFile(filepath.Join(p.basepath, name), bytes, 0o644)
	}
	return nil
}

// NewPersistForPath returns a Persist that loads and stores nodes as
// files in the directory at the given path.
//
//      p := NewPersistForPath("/var/db/users")
//      err, blob := p.load("98ea6e4f216f2fb4b69fff9b3a44842c38686ca685f3f55dc48c5d3fb1107be4")
func NewPersistForPath(path string) Persist {
	return Persist{path}
}
