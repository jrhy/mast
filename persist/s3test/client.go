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
			S3ForcePathStyle: aws.Bool(true),
		}
		// If AWS_REGION is set, we're using a real AWS S3
		// endpoint, so just let the SDK figure out which
		// endpoint to use. Otherwise, we're using min.io
		// or Wasabi or something else with an explicit
		// endpoint, and the AWS_REGION just needs to be
		// nonempty to satisfy the SDK.
		config.Region = aws.String(getEnvOrDefault("AWS_REGION", "not-using-AWS"))
		if *config.Region != "not-using-AWS" {
			config.Endpoint = nil
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
		closer = ts.Close

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

	bucketName := os.Getenv("JRHY_MAST_TEST_S3_BUCKET")
	if bucketName != "" {
		err := emptyBucket(client, bucketName)
		if err != nil {
			panic(err)
		}
	} else {
		bucketName = randBucketName()
		_, err := client.CreateBucket(&s3.CreateBucketInput{
			Bucket: &bucketName,
		})
		if err != nil {
			panic(err)
		}
	}

	oldCloser := closer
	closer = func() {
		oldCloser()
		emptyBucket(client, bucketName)
		if bucketName == "" {
			client.DeleteBucket(&s3.DeleteBucketInput{
				Bucket: &bucketName,
			})
		}
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

func emptyBucket(s *s3.S3, bucket string) error {
	params := &s3.ListObjectsInput{
		Bucket: &bucket,
	}
	for {
		objects, err := s.ListObjects(params)
		if err != nil {
			return err
		}
		if len((*objects).Contents) == 0 {
			return nil
		}
		objectsToDelete := make([]*s3.ObjectIdentifier, 0, 1000)
		for _, object := range objects.Contents {
			obj := s3.ObjectIdentifier{
				Key: object.Key,
			}
			objectsToDelete = append(objectsToDelete, &obj)
		}
		deleteParams := &s3.DeleteObjectsInput{
			Bucket: &bucket,
			Delete: &s3.Delete{
				Objects: objectsToDelete,
			},
		}
		_, err = s.DeleteObjects(deleteParams)
		if err != nil {
			return err
		}
		if *(*objects).IsTruncated {
			params.Marker = deleteParams.Delete.Objects[len(deleteParams.Delete.Objects)-1].Key
		} else {
			break
		}
	}
	return nil
}
