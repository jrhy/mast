package s3_test

import (
	"context"
	"testing"

	s3Persist "github.com/jrhy/mast/persist/s3"
	"github.com/jrhy/mast/persist/s3test"
	"github.com/stretchr/testify/require"
)

func TestHappyCase(t *testing.T) {
	t.Parallel()
	c, bucketName, closer := s3test.Client()
	t.Cleanup(closer)

	ctx := context.Background()
	p := s3Persist.NewPersist(c, bucketName, "node/")
	err := p.Store(ctx, "foofoo", []byte("here is some stuff"))
	require.NoError(t, err)
	b, err := p.Load(ctx, "foofoo")
	require.NoError(t, err)
	require.Equal(t, b, []byte("here is some stuff"))
}
