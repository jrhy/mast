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

func testS3Client() (*s3.S3, string, func()) {
	backend := s3mem.New()
	faker := gofakes3.New(backend)
	ts := httptest.NewServer(faker.Server())

	// configure S3 client
	s3Config := &aws.Config{
		Credentials: credentials.NewStaticCredentials(
			"TEST-ACCESSKEYID",
			"TEST-SECRETACCESSKEY",
			"",
		),
		Endpoint:         aws.String(ts.URL),
		Region:           aws.String("ca-west-1"),
		DisableSSL:       aws.Bool(true),
		S3ForcePathStyle: aws.Bool(true),
	}
	newSession := session.New(s3Config)
	bucketName := randBucketName()
	client := s3.New(newSession)
	client.CreateBucket(&s3.CreateBucketInput{
		Bucket: &bucketName,
	})
	return client, bucketName, func() { ts.Close() }
}

func randBucketName() string {
	i, err := rand.Int(rand.Reader, big.NewInt(math.MaxUint32))
	if err != nil {
		panic(err)
	}
	return fmt.Sprintf("bucket-%s", i)
}

func TestHappyCase(t *testing.T) {
	t.Parallel()
	c, bucketName, closer := testS3Client()
	defer closer()

	p := s3Persist.NewPersist(c, bucketName)
	err := p.Store(context.Background(), "foofoo", []byte("here is some stuff"))
	require.NoError(t, err)
	b, err := p.Load(context.Background(), "foofoo")
	require.NoError(t, err)
	assert.Equal(t, b, []byte("here is some stuff"))
}
