package s3test

import (
	"crypto/rand"
	"fmt"
	"math"
	"math/big"
	"net/http/httptest"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"
)

func Client() (*s3.S3, string, func()) {
	var client *s3.S3
	closer := func() {}
	if os.Getenv("JRHY_MAST_TEST_S3_ENDPOINT") != "" {
		config := aws.Config{
			Credentials: credentials.NewStaticCredentials(
				getEnv("AWS_ACCESS_KEY_ID"),
				getEnv("AWS_SECRET_ACCESS_KEY"),
				getEnvOrDefault("AWS_SESSION_TOKEN", ""),
			),
			Endpoint:         aws.String(getEnv("JRHY_MAST_TEST_S3_ENDPOINT")),
			Region:           aws.String(getEnv("AWS_DEFAULT_REGION")),
			S3ForcePathStyle: aws.Bool(true),
		}

		sess, err := session.NewSession(&config)
		if err != nil {
			panic(err)
		}
		client = s3.New(sess)
	} else {
		backend := s3mem.New()
		faker := gofakes3.New(backend)
		ts := httptest.NewServer(faker.Server())
		closer = func() { ts.Close() }

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
		client = s3.New(newSession)
	}
	bucketName := randBucketName()
	_, err := client.CreateBucket(&s3.CreateBucketInput{
		Bucket: &bucketName,
	})
	if err != nil {
		panic(err)
	}
	return client, bucketName, closer
}

func getEnv(key string) string {
	res := os.Getenv(key)
	if res == "" {
		panic(fmt.Sprintf("environment '%s' unset", key))
	}
	return res
}

func getEnvOrDefault(key, def string) string {
	res := os.Getenv(key)
	if res == "" {
		return def
	}
	return res
}

func randBucketName() string {
	i, err := rand.Int(rand.Reader, big.NewInt(math.MaxUint32))
	if err != nil {
		panic(err)
	}
	return fmt.Sprintf("bucket-%s", i)
}
