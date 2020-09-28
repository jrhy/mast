package s3_test

import (
	"context"
	"crypto/rand"
	"fmt"
	"math"
	"math/big"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"
	s3Persist "github.com/jrhy/mast/persist/s3"
	"github.com/stretchr/testify/assert"
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
