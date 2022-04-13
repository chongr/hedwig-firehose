package main

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
)

type ExampleTestSuite struct {
	suite.Suite
}

func (s *ExampleTestSuite) TestExample() {
	assert.Equal(s.T(), 2, 1+1)
}

func TestExampleTestSuite(t *testing.T) {
	suite.Run(t, new(ExampleTestSuite))
}

type FirehoseBackendMock struct {
	mock.Mock
}

func (b *FirehoseBackendMock) Receive(ctx context.Context, numMessages uint32, visibilityTimeout time.Duration, messageCh chan<- ReceivedMessage) error {
	return nil
}

func (b *FirehoseBackendMock) NackMessage(ctx context.Context, providerMetadata interface{}) error {
	return nil
}

func (b *FirehoseBackendMock) AckMessage(ctx context.Context, providerMetadata interface{}) error {
	return nil
}

func (b *FirehoseBackendMock) RequeueFirehoseDLQ(ctx context.Context, numMessages uint32, visibilityTimeout time.Duration) error {
	return nil
}
func (b *FirehoseBackendMock) UploadFile(ctx context.Context, data []byte, uploadBucket string, uploadLocation string) error {
	return nil
}

func (b *FirehoseBackendMock) ReadFile(ctx context.Context, readBucket string, readLocation string) ([]byte, error) {
	return []byte("abc"), nil
}

func TestNewFirehose(t *testing.T) {
	s := new(FirehoseBackendMock)
	var s2 ProcessSettings
	NewFirehose(s, s2)
}
