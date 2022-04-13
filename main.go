package main

import (
	"context"
	"fmt"
	"time"
)

type ProcessSettings struct {
	// interval when leader moves files to final bucket
	ScrapeInterval int

	// interval when follower flushes to staging bucket
	FlushAfter int
}

// ReceivedMessage is the message as received by a transport backend.
type ReceivedMessage struct {
	Payload          []byte
	Attributes       map[string]string
	ProviderMetadata interface{}
}

// FirehoseBackend is used for consuming messages from a transport and read/write to storage
type FirehoseBackend interface {
	Receive(ctx context.Context, numMessages uint32, visibilityTimeout time.Duration, messageCh chan<- ReceivedMessage) error

	// NackMessage nacks a message on the queue
	NackMessage(ctx context.Context, providerMetadata interface{}) error

	// AckMessage acknowledges a message on the queue
	AckMessage(ctx context.Context, providerMetadata interface{}) error

	// RequeueFirehoseDLQ re-queues everything in the firehose queue
	RequeueFirehoseDLQ(ctx context.Context, numMessages uint32, visibilityTimeout time.Duration) error
	UploadFile(ctx context.Context, data []byte, uploadBucket string, uploadLocation string) error

	ReadFile(ctx context.Context, readBucket string, readLocation string) ([]byte, error)
}

type Firehose struct {
	processSettings ProcessSettings
	firehoseBackend FirehoseBackend
}

func (fp *Firehose) RunFollower() {
}

func (fp *Firehose) RunLeader() {
}

// RunFirehose starts a Firehose running in leader of follower mode
func (f *Firehose) RunFirehose() {
	// 1. on start up determine if leader or followerBackend
	// 2. if leader call RunFollower
	// 3. else follower call RunFollower
}

func NewFirehose(firehoseBackend FirehoseBackend, processSettings ProcessSettings) *Firehose {
	// If we want to inject config from env, pass in when creating new
	f := &Firehose{
		processSettings: processSettings,
		firehoseBackend: firehoseBackend,
	}
	return f
}

func main() {
	fmt.Println("Hello World")
}
