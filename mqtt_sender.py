import threading
import queue
import time
import logging
import paho.mqtt.client as mqtt

class MqttSender(threading.Thread):
  def __init__(self, tname, req_q, host, topic):
    super(MqttSender, self).__init__(name=tname)
    self.q = req_q
    self.host = host
    self.topic = topic
    self.connected = False
    self.client = mqtt.Client()
    self.client.on_connect = self._mqtt_on_connect
    self.client.on_publish = self._mqtt_on_publish
    self.client.on_disconnect = self._mqtt_on_disconnect
    self.client.connect(host=self.host)
    self.stoprequest = threading.Event()
  
  def _mqtt_on_disconnect(self, client, userdata, rc):
    logging.info("Got disconnected from MQTT broker")
    self.connected = False

  def _mqtt_on_connect(self, client, userdata, flags, rc):
    logging.info("Connected to {host} with result code {rc}".format(host=self.host, rc=str(rc)))
    self.connected = True
  
  def _mqtt_on_publish(self, client, userdata, result):
    logging.info("Data published to topic {topic} with result code {rc}".format(topic=self.topic, rc=str(result)))

  def run(self):
    # need to start loop because this also manages keepalives in the background
    self.client.loop_start()
    while not self.stoprequest.isSet():
      try:
        message = self.q.get(True, 0.05)
        logging.info("Got message '{msg}' from Queue".format(msg=message))
        attempts = 0
        loop = True
        while loop and attempts < 3:
          if not self.connected:
            # we are not connected, but as we are using the managed loop we should reconnect automatically if we wait
            logging.info("Not connected to MQTT broker, so waiting before we attempt again")
            attempt = attempt + 1
            time.sleep(3)
          else:
            # we are connected, so proceed as normal
            logging.info("Connected to MQTT broker, so publishing")
            self.client.publish(self.topic, message)
            loop = False
        if loop:
          logging.info("We were not able to send and gave up after 3 attempts")
        else:
          logging.info("Message was sent")
      except queue.Empty:
        continue

  def join(self, timeout=None):
    logging.info("MqttSender setting exit flag")
    self.stoprequest.set()
    self.client.disconnect()
    super(MqttSender, self).join(timeout)