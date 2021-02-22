import time
import threading
import queue
import logging
import serial

class SerialHandler(threading.Thread):
  def __init__(self, req_q, send_q, com_port):
    super(SerialHandler, self).__init__()
    self.q = req_q
    self.send_q = send_q
    self.ser = serial.Serial(port=com_port, baudrate=9600, timeout=0.5)
    self.stoprequest = threading.Event()

  def run(self):
    while not self.stoprequest.isSet():
      data = self.ser.readline().rstrip(b"\r\n")
      if data:
        logging.info("Got data '{d}' from serial port".format(d=data.decode("utf-8")))
        self.q.put("{d}".format(d=data.decode("utf-8")))
      else:
        logging.debug("No data on serial port, checking send queue...")
        try:
          message = self.send_q.get(True, 0.05)
          logging.info("Got message '{msg}' from Queue to send to serial port".format(msg=message))
          if message == "open":
            logging.info("Setting door lock to open")
            self.ser.write(b"OP\n")
          elif message == "close":
            logging.info("Setting door lock to closed")
            self.ser.write(b"CL\n")
        except queue.Empty:
          continue
			
  def join(self, timeout=None):
    logging.info("SerialHandler setting exit flag")
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
    logging.info("FakeSerial setting exit flag")
    self.stoprequest.set()
    super(FakeSerial, self).join(timeout)