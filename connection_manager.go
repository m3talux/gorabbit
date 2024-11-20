package gorabbit

import (
	"context"
	"time"
)

type connectionManager struct {
	// consumerConnection holds the independent consuming connection.
	consumerConnection *amqpConnection

	// publisherConnection holds the independent publishing connection.
	publisherConnection *amqpConnection

	// marshaller holds the marshaller used to encode messages.
	marshaller Marshaller
}

// newConnectionManager instantiates a new connectionManager with given arguments.
func newConnectionManager(
	ctx context.Context,
	uri string,
	connectionName string,
	keepAlive bool,
	retryDelay time.Duration,
	maxRetry uint,
	publishingCacheSize uint64,
	publishingCacheTTL time.Duration,
	logger logger,
	marshaller Marshaller,
) *connectionManager {
	if connectionName == "" {
		connectionName = libraryName
	}

	c := &connectionManager{
		consumerConnection: newConsumerConnection(
			ctx, uri, connectionName, keepAlive, retryDelay, logger, marshaller,
		),
		publisherConnection: newPublishingConnection(
			ctx, uri, connectionName, keepAlive, retryDelay, maxRetry,
			publishingCacheSize, publishingCacheTTL, logger, marshaller,
		),
		marshaller: marshaller,
	}

	return c
}

// close offers the basic connection and channel close() mechanism but with extra higher level checks.
func (c *connectionManager) close() error {
	if err := c.publisherConnection.close(); err != nil {
		return err
	}

	return c.consumerConnection.close()
}

// isReady returns true if both consumerConnection and publishingConnection are ready.
func (c *connectionManager) isReady() bool {
	if c.publisherConnection == nil || c.consumerConnection == nil {
		return false
	}

	return c.publisherConnection.ready() && c.consumerConnection.ready()
}

// isHealthy returns true if both consumerConnection and publishingConnection are healthy.
func (c *connectionManager) isHealthy() bool {
	if c.publisherConnection == nil || c.consumerConnection == nil {
		return false
	}

	return c.publisherConnection.healthy() && c.consumerConnection.healthy()
}

// registerConsumer registers a new MessageConsumer.
func (c *connectionManager) registerConsumer(consumer MessageConsumer) error {
	if c.consumerConnection == nil {
		return errConsumerConnectionNotInitialized
	}

	return c.consumerConnection.registerConsumer(consumer)
}

func (c *connectionManager) publish(exchange, routingKey string, payload interface{}, options *PublishingOptions) error {
	if c.publisherConnection == nil {
		return errPublisherConnectionNotInitialized
	}

	payloadBytes, err := c.marshaller.Marshal(payload)
	if err != nil {
		return err
	}

	return c.publisherConnection.publish(exchange, routingKey, payloadBytes, options)
}
