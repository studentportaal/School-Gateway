from google.cloud import pubsub_v1
import time
import sys
import requests

project_id = 'pts6-bijbaan'
topic_name = 'generic-fontys'
subscription_name = 'fontys-generic'

publisher = pubsub_v1.PublisherClient()
subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(
    project_id, subscription_name
)
topic_path = publisher.topic_path(project_id, topic_name)

data = 'test'
data = data.encode('utf-8')
future = publisher.publish(topic_path, data=data)
print('Published {} of message ID {}'.format(data.decode('UTF-8'), future.result()))


def callback(message):
    print('Received message {}'.format(message))

    if message.attributes:
        if message.attributes.get('user') == 'unknown':
            # TODO message contains data about user that needs to be posted to backend
            print('unknown user')
        elif message.attributes.get('user') == 'known':
            # TODO post to backend get user by id
            print('known user')
    else:
        print(message.data.decode('utf-8'))
    message.ack()


def main(argv=None):
    init()


def init():
    subscriber.subscribe(subscription_path, callback=callback)
    print('listening for messages on {}'.format(subscription_path))

    while True:
        time.sleep(1)


if __name__ == "__main__":
    main(sys.argv)