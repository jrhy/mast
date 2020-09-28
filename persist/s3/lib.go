package s3

import (
	"bytes"
	"context"
	"io/ioutil"

	"github.com/aws/aws-sdk-go/service/s3"
)

// Persist implements the mast.Persist interface for storing and loading
// nodes from files.
type Persist struct {
	S3         *s3.S3
	BucketName string
}

// Load loads the bytes persisted in the named object.
func (p Persist) Load(ctx context.Context, name string) ([]byte, error) {
	input := s3.GetObjectInput{
		Bucket: &p.BucketName,
		Key:    &name,
	}
	output, err := p.S3.GetObjectWithContext(ctx, &input)
	if err != nil {
		return nil, err
	}
	return ioutil.ReadAll(output.Body)
}

// Store persists the given bytes in a file of the given name, if it
// doesn't exist already.
func (p Persist) Store(ctx context.Context, name string, b []byte) error {
	input := s3.PutObjectInput{
		Bucket: &p.BucketName,
		Key:    &name,
		Body:   bytes.NewReader(b),
	}
	_, err := p.S3.PutObjectWithContext(ctx, &input)
	if err != nil {
		return err
	}
	return nil
}

// NewPersist returns a Persist that loads and stores nodes as
// objects with the given S3 client and bucket name.
func NewPersist(client *s3.S3, bucketName string) Persist {
	return Persist{client, bucketName}
}
