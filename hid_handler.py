import time
import threading
import queue
import logging
import serial
import sys
import re
import json

class HandleHIDCode(threading.Thread):
  def __init__(self, req_q, mqtt_pub_queue, door_name):
    super(HandleHIDCode, self).__init__()
    self.q = req_q
    self.door_name = door_name
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
    if m:
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
    else:
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
      "card_code": card_code,
      "door_name": self.door_name
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
      "card_code": card_code,
      "door_name": self.door_name
    }

  def join(self, timeout=None):
    logging.info("HandleHIDCode setting exit flag")
    self.stoprequest.set()
    super(HandleHIDCode, self).join(timeout)