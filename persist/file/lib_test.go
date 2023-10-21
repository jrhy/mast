package file

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var ctx = context.Background()

func TestFiles(t *testing.T) {
	dir, err := os.MkdirTemp("", "test")
	require.NoError(t, err)

	p := NewPersistForPath(dir)

	err = p.Store(ctx, "foo", []byte("hello"))
	require.NoError(t, err)
	loaded, err := p.Load(ctx, "foo")
	require.NoError(t, err)
	assert.Equal(t, []byte("hello"), loaded)

	if !t.Failed() {
		os.RemoveAll(dir)
	} else {
		fmt.Println("temp directory:", dir)
	}
}
