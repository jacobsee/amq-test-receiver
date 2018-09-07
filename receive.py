from __future__ import print_function

import sys
import os

from proton.handlers import MessagingHandler
from proton.reactor import Container, Selector

class ReceiveHandler(MessagingHandler):
    def __init__(self, conn_url, address, min_priority = 0, max_priority = 9, name = ''):
        super(ReceiveHandler, self).__init__()

        self.conn_url = conn_url
        self.address = address
        self.min_priority = min_priority
        self.max_priority = max_priority
        self.name = name

    def on_start(self, event):
        conn = event.container.connect(self.conn_url)
        event.container.create_receiver(
            conn, 
            self.address, 
            options=Selector("JMSPriority >= {0} AND JMSPriority <= {1}".format(self.min_priority, self.max_priority))
        )

    def on_link_opened(self, event):
        print("RECEIVE: Created receiver for source address '{0}'".format
              (self.address))

    def on_message(self, event):
        message = event.message

        print("{0} RECEIVE: Received message '{1}' with a priority of {2}".format(self.name, message.body, message.priority))

        if False: # If we want to stop receiving... but let's keep going
            event.receiver.close()
            event.connection.close()

def main():
    try:
        conn_url, address, min_priority, max_priority, name = sys.argv[1:6]
    except ValueError:
        sys.exit("Usage: receive.py CONNECTION-URL ADDRESS MIN-PRIORITY MAX-PRIORITY RECEIVER-NAME")

    try:
        min_priority = int(sys.argv[3])
    except (IndexError, ValueError):
        min_priority = 0

    try:
        max_priority = int(sys.argv[4])
    except (IndexError, ValueError):
        max_priority = 9

    handler = ReceiveHandler(conn_url, os.getenv('ACTIVEMQ_ADDRESS', address), min_priority, max_priority, name)
    container = Container(handler)
    container.run()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass