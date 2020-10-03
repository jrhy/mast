package s3

import (
	"bytes"
	"context"
	"io/ioutil"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/hashicorp/golang-lru/simplelru"
)

type S3Interface interface {
	DeleteObjectWithContext(ctx aws.Context, input *s3.DeleteObjectInput, opts ...request.Option) (*s3.DeleteObjectOutput, error)
	GetObjectWithContext(ctx aws.Context, input *s3.GetObjectInput, opts ...request.Option) (*s3.GetObjectOutput, error)
	PutObjectWithContext(ctx aws.Context, input *s3.PutObjectInput, opts ...request.Option) (*s3.PutObjectOutput, error)
}

// Persist implements the mast.Persist interface for storing and loading
// nodes from files.
type Persist struct {
	s3         S3Interface
	BucketName string
	Prefix     string
	lru        *simplelru.LRU
}

// Load loads the bytes persisted in the named object.
func (p *Persist) Load(ctx context.Context, name string) ([]byte, error) {
	input := s3.GetObjectInput{
		Bucket: &p.BucketName,
		Key:    aws.String(p.Prefix + name),
	}
	output, err := p.s3.GetObjectWithContext(ctx, &input)
	if err != nil {
		return nil, err
	}
	b, err := ioutil.ReadAll(output.Body)
	if err != nil {
		return nil, err
	}
	p.lru.Add(name, nil)
	return b, nil
}

// Store persists the given bytes in a file of the given name, if it
// doesn't exist already.
func (p Persist) Store(ctx context.Context, name string, b []byte) error {
	if _, present := p.lru.Get(name); present {
		return nil
	}
	input := s3.PutObjectInput{
		Bucket: &p.BucketName,
		Key:    aws.String(p.Prefix + name),
		Body:   bytes.NewReader(b),
	}
	_, err := p.s3.PutObjectWithContext(ctx, &input)
	if err != nil {
		return err
	}
	return nil
}

// NewPersist returns a Persist that loads and stores nodes as
// objects with the given S3 client and bucket name.
func NewPersist(client S3Interface, bucketName, prefix string) Persist {
	lru, err := simplelru.NewLRU(1000, nil)
	if err != nil {
		panic(err)
	}
	return Persist{client, bucketName, prefix, lru}
}
