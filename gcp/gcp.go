package gcp

import (
	"context"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/pkg/errors"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/option"
)

// This should satify interface for FirehoseBackend
type Backend struct {
	client   *pubsub.Client
	settings Settings
}

// SubscriptionProject represents a tuple of subscription name and project for cross-project Google subscriptions
type SubscriptionProject struct {
	// Subscription name
	Subscription string

	// ProjectID
	ProjectID string
}

func (sp *SubscriptionProject) GetIdentifer() string {
}

// Metadata is additional metadata associated with a message
type Metadata struct {
	// Underlying pubsub message - ack id isn't exported so we have to store this object
	pubsubMessage *pubsub.Message

	// PublishTime is the time this message was originally published to Pub/Sub
	PublishTime time.Time

	// DeliveryAttempt is the counter received from Pub/Sub.
	//    The first delivery of a given message will have this value as 1. The value
	//    is calculated as best effort and is approximate.
	DeliveryAttempt int
}

// Settings for Hedwig firehose
type Settings struct {
	// bucket where leader file is saved
	MetadataBucket string

	// bucket where follower put intermediate files to be moved by leader
	StagingBucket string

	// final bucket for firehose files
	OutputBucket string
	// Firehose queue name, for requeueing
	QueueName string

	// GoogleCloudProject ID that contains Pub/Sub resources.
	GoogleCloudProject string

	// PubsubClientOptions is a list of options to pass to pubsub.NewClient. This may be useful to customize GRPC
	// behavior for example.
	PubsubClientOptions []option.ClientOption

	// FirehoseSubscriptions is a list of tuples of topic name and GCP project for project topic messages.
	// Google only.
	FirehoseSubscriptions []SubscriptionProject
}

func (b *Backend) initDefaults() {
	if b.settings.PubsubClientOptions == nil {
		b.settings.PubsubClientOptions = []option.ClientOption{}
	}
}

func (b *Backend) ensureClient(ctx context.Context) error {
	googleCloudProject := b.settings.GoogleCloudProject
	if googleCloudProject == "" {
		creds, err := google.FindDefaultCredentials(ctx)
		if err != nil {
			return errors.Wrap(
				err, "unable to discover google cloud project setting, either pass explicitly, or fix runtime environment")
		} else if creds.ProjectID == "" {
			return errors.New(
				"unable to discover google cloud project setting, either pass explicitly, or fix runtime environment")
		}
		googleCloudProject = creds.ProjectID
	}
	if b.client != nil {
		return nil
	}
	client, err := pubsub.NewClient(context.Background(), googleCloudProject, b.settings.PubsubClientOptions...)
	if err != nil {
		return err
	}
	b.client = client
	return nil
}

func (b *Backend) Receive(ctx context.Context, queueIdentifier QueueIdentifer, numMessages uint32, visibilityTimeout time.Duration, messageCh chan<- hedwig.ReceivedMessage) {
	err := b.ensureClient(ctx)
	if err != nil {
		return err
	}

	defer b.client.Close()

	subscription := queueIdentifier.GetIdentifer()

	group, gctx := errgroup.WithContext(ctx)

	pubsubSubscription := b.client.Subscription(subscription)
	pubsubSubscription.ReceiveSettings.NumGoroutines = 1
	pubsubSubscription.ReceiveSettings.MaxOutstandingMessages = int(numMessages)
	if visibilityTimeout != 0 {
		pubsubSubscription.ReceiveSettings.MaxExtensionPeriod = visibilityTimeout
	} else {
		pubsubSubscription.ReceiveSettings.MaxExtensionPeriod = defaultVisibilityTimeoutS
	}
	group.Go(func() error {
		recvErr := pubsubSubscription.Receive(gctx, func(ctx context.Context, message *pubsub.Message) {
			metadata := Metadata{
				pubsubMessage:   message,
				PublishTime:     message.PublishTime,
				DeliveryAttempt: *message.DeliveryAttempt,
			}
			messageCh <- hedwig.ReceivedMessage{
				Payload:          message.Data,
				Attributes:       message.Attributes,
				ProviderMetadata: metadata,
			}
		})
		return recvErr
	})

	err = group.Wait()
	if err != nil {
		return err
	}
	// context cancelation doesn't return error in the group
	return ctx.Err()
}

func (b *Backend) NackMessage(ctx context.Context, providerMetadata interface{}) error {
	providerMetadata.(Metadata).pubsubMessage.Nack()
	return nil
}

func (b *Backend) AckMessage(ctx context.Context, providerMetadata interface{}) error {
	providerMetadata.(Metadata).pubsubMessage.Ack()
	return nil
}

// TODO: add when implementing requeue and processing DLQ logijc
func (b *Backend) RequeueDLQ(ctx context.Context, numMessages uint32, visibilityTimeout time.Duration) error {
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
func NewBackend(settings Settings) *Backend {
	b := &Backend{settings: settings}
	b.initDefaults()
	return b
}
