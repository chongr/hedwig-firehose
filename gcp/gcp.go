package gcp

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"time"

	"cloud.google.com/go/storage"
	"github.com/cloudchacho/hedwig-go"
	"github.com/cloudchacho/hedwig-go/gcp"
)

// This should satify interface for FirehoseBackend
type Backend struct {
	*gcp.Backend
	firehoseSettings FirehoseSettings
}

// Settings for Hedwig firehose
type FirehoseSettings struct {
	// bucket where leader file is saved
	MetadataBucket string

	// bucket where follower put intermediate files to be moved by leader
	StagingBucket string

	// final bucket for firehose files
	OutputBucket string

	// Firehose queue name, for requeueing
	FirehoseQueueName string
}

// TODO: add when implementing requeue and processing DLQ logijc
func (b *Backend) RequeueFirehoseDLQ(ctx context.Context, numMessages uint32, visibilityTimeout time.Duration) error {
	return nil
}

func (b *Backend) UploadFile(ctx context.Context, data []byte, uploadBucket string, uploadLocation string) error {
	client, err := storage.NewClient(ctx)
	if err != nil {
		return fmt.Errorf("storage.NewClient: %v", err)
	}
	defer client.Close()

	buf := bytes.NewBuffer(data)

	ctx, cancel := context.WithTimeout(ctx, time.Second*50)
	defer cancel()

	// Upload an object with storage.Writer.
	wc := client.Bucket(uploadBucket).Object(uploadLocation).NewWriter(ctx)
	wc.ChunkSize = 0

	if _, err = io.Copy(wc, buf); err != nil {
		return fmt.Errorf("io.Copy: %v", err)
	}
	// Data can continue to be added to the file until the writer is closed.
	if err := wc.Close(); err != nil {
		return fmt.Errorf("Writer.Close: %v", err)
	}

	return nil
}

func (b *Backend) ReadFile(ctx context.Context, readBucket string, readLocation string) ([]byte, error) {
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("storage.NewClient: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(ctx, time.Second*50)
	defer cancel()

	rc, err := client.Bucket(readBucket).Object(readLocation).NewReader(ctx)
	if err != nil {
		return nil, fmt.Errorf("Object(%q).NewReader: %v", readLocation, err)
	}
	defer rc.Close()

	data, err := ioutil.ReadAll(rc)
	if err != nil {
		return nil, fmt.Errorf("ioutil.ReadAll: %v", err)
	}
	return data, nil
}

// NewBackend creates a Firehose on GCP
// The provider metadata produced by this Backend will have concrete type: gcp.Metadata
func NewBackend(firehoseSettings FirehoseSettings, hedwigSettings gcp.Settings, getLogger hedwig.GetLoggerFunc) *Backend {
	b := &Backend{
		gcp.NewBackend(hedwigSettings, getLogger),
		firehoseSettings,
	}
	return b
}
