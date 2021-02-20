import time
import threading
import queue
import logging
import serial
import sys
import re
import json
import paho.mqtt.client as mqtt

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s [%(levelname)s] (%(threadName)-10s) %(message)s')

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
    logging.info("Setting exit flag")
    self.stoprequest.set()
    self.client.disconnect()
    super(MqttSender, self).join(timeout)

class HandleHIDCode(threading.Thread):
  def __init__(self, req_q, mqtt_pub_queue):
    super(HandleHIDCode, self).__init__()
    self.q = req_q
    self.mqtt_pub_queue = mqtt_pub_queue
    self.stoprequest = threading.Event()

  def run(self):
    while not self.stoprequest.isSet():
      try:
        hid_message = self.q.get(True, 0.05)
        logging.info("Got HID message '{msg}' from Queue".format(msg=hid_message))
        result = self.process_hid_code(hid_message)
        if result:
          self.mqtt_pub_queue.put(json.dumps(result))
        else:
          logging.info("Card was not in a recognised format.")
      except queue.Empty:
        continue

  def process_hid_code(self, hid_code):
    logging.info("Processing HID code '{msg}'".format(msg=hid_code))
    p = re.compile("\[(\d+),([A-Fa-z0-9]+)\]")
    m = p.match(hid_code)
    number_of_bits = m.group(1)  
    card_data = m.group(2)
    logging.info("Number of bits on card = {bits}, card data = '{data}'".format(bits=number_of_bits, data=card_data))
    if(int(number_of_bits) == 26):
      logging.info("This is a 26-bit HID card, the standard open format")
      return self.process_26bit_card(card_data)
    if(int(number_of_bits) == 35):
      logging.info("This is a 35-bit HID card, the Corporate 1000 format")
      return self.process_35bit_card(card_data)
    return False
	
  def process_26bit_card(self, hid_code):
    logging.info("Processing 26 bit card")
    hid_number = int(hid_code, 16)
    logging.info("Card number in decimal is '{num}'".format(num=hid_number))
    facility_code_mask = int("1111111100000000000000000", 2)
    card_code_mask     = int("0000000011111111111111110", 2)
    facility_code = hid_number & facility_code_mask
    facility_code = facility_code >> 17
    card_code = hid_number & card_code_mask
    card_code = card_code >> 1
    logging.info("Facility code = {fc}, card code = {cc}".format(fc=facility_code, cc=card_code))
    return {
      "type": "26-bit",
      "time": str(time.time()),
      "facility_code": facility_code,
      "card_code": card_code
    }

  def process_35bit_card(self, hid_code):
    logging.info("Processing 35 bit card")
    hid_number = int(hid_code, 16)
    logging.info("Card number in decimal is '{num}'".format(num=hid_number))
    facility_code_mask = int("11111111111000000000000000000000", 2)
    card_code_mask     = int("00000000000111111111111111111110", 2)
    facility_code = hid_number & facility_code_mask  
    facility_code = facility_code >> 21
    card_code = hid_number & card_code_mask
    card_code = card_code >> 1
    logging.info("Facility code = {fc}, card code = {cc}".format(fc=facility_code, cc=card_code))
    return {
      "type": "35-bit",
      "time": str(time.time()),
      "facility_code": facility_code,
      "card_code": card_code
    }

  def join(self, timeout=None):
    logging.info("Setting exit flag")
    self.stoprequest.set()
    super(HandleHIDCode, self).join(timeout)

class SerialHandler(threading.Thread):
  def __init__(self, req_q, com_port):
    super(SerialHandler, self).__init__()
    self.q = req_q
    self.ser = serial.Serial(com_port, 9600)
    self.stoprequest = threading.Event()

  def run(self):
    while not self.stoprequest.isSet():
      data = self.ser.readline().rstrip(b"\r\n")
      logging.info("Got data '{d}' from serial port".format(d=data.decode("utf-8")))
      self.q.put("{d}".format(d=data.decode("utf-8")))
			
  def join(self, timeout=None):
    logging.info("Setting exit flag")
    self.stoprequest.set()
    super(SerialHandler, self).join(timeout)

class FakeSerial(threading.Thread):
  def __init__(self, req_q):
    super(FakeSerial, self).__init__()
    self.q = req_q
    self.stoprequest = threading.Event()

  def run(self):
    while not self.stoprequest.isSet():
      time.sleep(10)
      data = "[26,3151EAA]"
      logging.info("Putting fake data '{d}' onto queue".format(d=data))
      self.q.put("{d}".format(d=data))
			
  def join(self, timeout=None):
    logging.info("Setting exit flag")
    self.stoprequest.set()
    super(FakeSerial, self).join(timeout)

def run_program():
  serial_out_queue = queue.Queue()
  mqtt_pub_queue = queue.Queue()
  hid_thread = HandleHIDCode(req_q=serial_out_queue, mqtt_pub_queue=mqtt_pub_queue)
  #ser_thread = SerialHandler(req_q=serial_out_queue, com_port="COM3")
  ser_thread = FakeSerial(req_q=serial_out_queue)
  mqtt_send_thread = MqttSender(req_q=mqtt_pub_queue, host="localhost", topic="house/storage/door1/hidreader")

  try:
    # HID code handling thread
    hid_thread.setDaemon(True)
    hid_thread.start()
    logging.info("HID handling thread has started")

    # Serial handling thread
    ser_thread.setDaemon(True)
    ser_thread.start()
    logging.info("Serial handling thread has started")

    # MQTT thread
    mqtt_send_thread.setDaemon(True)
    mqtt_send_thread.start()
    logging.info("MQTT send thread started")

    # keep waiting
    while(True):
      time.sleep(5)
      logging.debug("Main thread running")

  except (KeyboardInterrupt, SystemExit):
    logging.info("Cleaning up... killing threads")
    ser_thread.join(3)
    hid_thread.join(3)
    mqtt_send_thread.join(3)
    sys.exit()

if __name__ == '__main__':
  logging.info("Main method, starting")
  run_program()