import time
import threading
import logging

import paho.mqtt.client as mqtt

class MqttReceiver(threading.Thread):
  def __init__(self, send_q, host, topic):
    super(MqttReceiver, self).__init__()
    self.q = send_q
    self.host = host
    self.topic = topic
    self.client = mqtt.Client()
    self.client.on_connect = self._mqtt_on_connect
    self.client.on_message = self._mqtt_on_message
    self.client.connect(host=self.host)
    self.stoprequest = threading.Event()
  
  def _mqtt_on_connect(self, client, userdata, flags, rc):
    logging.info("Connected to {host} with result code {rc}, subscribing to {topic}...".format(host=self.host, rc=str(rc), topic=self.topic))
    self.client.subscribe(self.topic)
  
  def _mqtt_on_message(self, client, userdata, msg):
    logging.info("Got a message from {topic}".format(topic=msg.topic))
    payload = msg.payload.decode("utf-8")
    logging.info("Payload = '{payload}', putting it on local queue".format(payload=msg.payload))
    self.q.put(payload)
  
  def run(self):
    self.client.loop_start()
    while not self.stoprequest.isSet():
      logging.debug("MqttReceiver thread is running")
      time.sleep(5)

  def join(self, timeout=None):
    logging.info("MqttReceiver setting exit flag")
    self.stoprequest.set()
    self.client.disconnect()
    super(MqttReceiver, self).join(timeout)