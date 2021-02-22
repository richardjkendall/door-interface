import threading
import queue
import logging
import paho.mqtt.client as mqtt

class MqttSender(threading.Thread):
  def __init__(self, req_q, host, topic):
    super(MqttSender, self).__init__()
    self.q = req_q
    self.host = host
    self.topic = topic
    self.client = mqtt.Client()
    self.client.on_connect = self._mqtt_on_connect
    self.client.on_publish = self._mqtt_on_publish
    self.client.connect(host=self.host)
    self.stoprequest = threading.Event()
  
  def _mqtt_on_connect(self, client, userdata, flags, rc):
    logging.info("Connected to {host} with result code {rc}".format(host=self.host, rc=str(rc)))
  
  def _mqtt_on_publish(self, client, userdata, result):
    logging.info("Data published to topic {topic} with result code {rc}".format(topic=self.topic, rc=str(result)))

  def run(self):
    while not self.stoprequest.isSet():
      try:
        message = self.q.get(True, 0.05)
        logging.info("Got message '{msg}' from Queue".format(msg=message))
        self.client.publish(self.topic, message)
      except queue.Empty:
        continue

  def join(self, timeout=None):
    logging.info("MqttSender setting exit flag")
    self.stoprequest.set()
    self.client.disconnect()
    super(MqttSender, self).join(timeout)