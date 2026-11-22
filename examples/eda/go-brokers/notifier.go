package main

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns"
)

type Notifier struct {
	sns *sns.Client
}

func (n *Notifier) Announce(ctx context.Context, orderID string) error {
	_, err := n.sns.Publish(ctx, &sns.PublishInput{TopicArn: aws.String("arn:aws:sns:eu-west-1:123456789012:order-events"), Message: aws.String(orderID)})
	return err
}
