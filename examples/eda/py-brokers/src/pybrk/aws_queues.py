import boto3

sqs = boto3.client("sqs", region_name="eu-west-1")
sns = boto3.client("sns")
ORDERS_URL = "https://sqs.eu-west-1.amazonaws.com/123456789012/orders"
TOPIC_ARN = "arn:aws:sns:eu-west-1:123456789012:order-events"


def enqueue(body):
    sqs.send_message(QueueUrl=ORDERS_URL, MessageBody=body)  # orders


def poll():
    return sqs.receive_message(QueueUrl=ORDERS_URL, MaxNumberOfMessages=5)  # orders


def fan_out(message):
    sns.publish(TopicArn=TOPIC_ARN, Message=message)  # order-events


session = boto3.Session()
refunds = session.client("sqs")
refunds_url = refunds.get_queue_url(QueueName="refunds")["QueueUrl"]
refunds.send_message(QueueUrl=refunds_url, MessageBody="start")  # refunds
