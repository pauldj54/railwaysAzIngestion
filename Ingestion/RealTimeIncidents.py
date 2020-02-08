import stomp
import time
import socket
from pathlib import Path
import xmltodict
import json

# Load Config file
with open("Ingestion\settings.json") as f:
  config = json.load(f)
print(config)

#USERNAME = 'KB9c88fe8b-a5d7-4b66-b85f-1b942bbfb302'
USERNAME = config["userName"]
PASSWORD = config["password"]
HOSTNAME  = config["hostName"]
# Always prefixed by /topic/ (it's not a queue, it's a topic)
TOPIC = config["topic"]
HOSTPORT  = config["hostPort"]
OPEN_WIRE_PORT = config["openWirePort"]

CLIENT_ID = socket.getfqdn()
HEARTBEAT_INTERVAL_MS = config["heartBeatInterval"]
RECONNECT_DELAY_SECS = config["reconnectDelay"]

if USERNAME == '':
    raise Exception("Please configure your username and password in opendata-nationalrail-client.py!")


def connect_and_subscribe(connection):

    connect_header = {'client-id': USERNAME + '-' + CLIENT_ID}
    subscribe_header = {'activemq.subscriptionName': CLIENT_ID}

    connection.connect(username=USERNAME,
                       passcode=PASSWORD,
                       wait=True,
                       headers=connect_header)

    connection.subscribe(destination=TOPIC,
                         id='1',
                         ack='auto',
                         headers=subscribe_header)


class StompClient(stomp.ConnectionListener):

    def on_heartbeat(self):
        print('Received a heartbeat')

    def on_heartbeat_timeout(self):
        print('ERROR: Heartbeat timeout')

    def on_error(self, headers, message):
        print('ERROR: %s' % message)

    def on_disconnected(self):
        print('Disconnected waiting %s seconds before exiting' % RECONNECT_DELAY_SECS)
        time.sleep(RECONNECT_DELAY_SECS)
        exit(-1)

    def on_connecting(self, host_and_port):
        print('Connecting to ' + host_and_port[0])

    def on_message(self, headers, message):
        try:
            print('\n---\nGot message %s' % message)
        except Exception as e:
            print("\n\tError: %s\n--------\n" % str(e))


conn = stomp.Connection12([(HOSTNAME, HOSTPORT)],
                          auto_decode=False,
                          heartbeats=(HEARTBEAT_INTERVAL_MS, HEARTBEAT_INTERVAL_MS))

conn.set_listener('', StompClient())
connect_and_subscribe(conn)

# Loop forever and wait one second
while True:
    time.sleep(1)

conn.disconnect()