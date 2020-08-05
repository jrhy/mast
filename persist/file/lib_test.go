package file

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jrhy/mast"
)

func TestFiles(t *testing.T) {
	dir, err := ioutil.TempDir("", "test")
	require.NoError(t, err)

	p := NewPersistForPath(dir)
	var _ mast.Persist = p

	err = p.Store("foo", []byte("hello"))
	require.NoError(t, err)
	loaded, err := p.Load("foo")
	require.NoError(t, err)
	assert.Equal(t, []byte("hello"), loaded)

	if !t.Failed() {
		os.RemoveAll(dir)
	} else {
		fmt.Println("temp directory:", dir)
	}
}
