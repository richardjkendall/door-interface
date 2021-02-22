import threading
import time
import queue
import logging
from decouple import config
import paho.mqtt.client as mqtt

door_timeout = int(config("DOOR_OPEN_TIME"))

class DoorHandler(threading.Thread):
  def __init__(self, tname, message_in_q, message_out_q, serial_q):
    super(DoorHandler, self).__init__(name=tname)
    self.message_in_q = message_in_q
    self.message_out_q = message_out_q
    self.serial_q = serial_q
    self.stoprequest = threading.Event()

  def run(self):
    while not self.stoprequest.isSet():
      try:
        message = self.message_in_q.get(True, 0.05)
        logging.info("DoorHandler: got message '{msg}'".format(msg=message))
        if message == "open":
          logging.info("Opening door...")
          self.serial_q.put("open")
          time.sleep(door_timeout)
          logging.info("Closing door...")
          self.message_out_q.put("close")
        else:
          self.serial_q.put("close")
      except queue.Empty:
        continue

  def join(self, timeout=None):
    logging.info("DoorHandler setting exit flag")
    self.stoprequest.set()
    super(DoorHandler, self).join(timeout)