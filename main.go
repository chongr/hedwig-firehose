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

// QueueIdentifer provides a function to get the identifier for a backend to sub to a queue
// structs satisfying FirehoseBackend should have FirehoseSubscriptions filled with QueueIdentifers
type QueueIdentifer interface {
	GetIdentifer() string
}

// FirehoseBackend is used for consuming messages from a transport and read/write to storage
type FirehoseBackend interface {
	// Receive messages from configured queue(s) and provide it through the channel. This should run indefinitely
	// until the context is canceled. Provider metadata should include all info necessary to ack/nack a message.
	// The channel must not be closed by the backend.
	Receive(ctx context.Context, queueIdentifier QueueIdentifer, numMessages uint32, visibilityTimeout time.Duration, messageCh chan<- ReceivedMessage) error

	// NackMessage nacks a message on the queue
	NackMessage(ctx context.Context, providerMetadata interface{}) error

	// AckMessage acknowledges a message on the queue
	AckMessage(ctx context.Context, providerMetadata interface{}) error

	// RequeueDLQ re-queues everything in the Hedwig DLQ back into the Hedwig queue
	RequeueDLQ(ctx context.Context, numMessages uint32, visibilityTimeout time.Duration) error
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
