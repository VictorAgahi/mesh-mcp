package main

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

type OrderQueue struct {
	sqs      *sqs.Client
	notifier *Notifier
}

func (q *OrderQueue) Enqueue(ctx context.Context, orderID string) error {
	_, err := q.sqs.SendMessage(ctx, &sqs.SendMessageInput{QueueUrl: aws.String("https://sqs.eu-west-1.amazonaws.com/123456789012/orders"), MessageBody: aws.String(orderID)})
	return err
}

func (q *OrderQueue) Poll(ctx context.Context) error {
	out, err := q.sqs.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: aws.String("https://sqs.eu-west-1.amazonaws.com/123456789012/orders")})
	if err != nil {
		return err
	}
	for _, m := range out.Messages {
		if err := q.notifier.Announce(ctx, *m.Body); err != nil {
			return err
		}
	}
	return nil
}
