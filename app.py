import time
#import threading
import queue
import logging
import sys
from decouple import config

from mqtt_receiver import MqttReceiver
from mqtt_sender import MqttSender
from hid_handler import HandleHIDCode
from serial_handler import SerialHandler, FakeSerial
from door_handler import DoorHandler

# setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] (%(threadName)-10s) %(message)s')

def run_program():
  # get config
  door_name = config("DOOR_NAME")
  com_port = config("COM_PORT")
  mqtt_broker_host = config("MQTT_BROKER_HOST")
  mqtt_door_hid_topic = config("MQTT_DOOR_STATUS_TOPIC")
  mqtt_door_lock_topic = config("MQTT_DOOR_LOCK_TOPIC")
  log_level = config("LOGGING")

  # create internal queues
  serial_out_queue = queue.Queue()
  mqtt_pub_queue = queue.Queue()
  serial_in_queue = queue.Queue()
  door_lock_in_queue = queue.Queue()
  door_lock_out_queue = queue.Queue()

  # create threads
  ser_thread = SerialHandler(req_q=serial_out_queue, send_q=serial_in_queue, com_port=com_port)
  fake_ser_thread = FakeSerial(req_q=serial_out_queue)
  hid_thread = HandleHIDCode(req_q=serial_out_queue, mqtt_pub_queue=mqtt_pub_queue, door_name=door_name)
  mqtt_send_thread = MqttSender(req_q=mqtt_pub_queue, host=mqtt_broker_host, topic=mqtt_door_hid_topic)
  mqtt_rec_thread = MqttReceiver(send_q=door_lock_in_queue, host=mqtt_broker_host, topic=mqtt_door_lock_topic)
  door_handler = DoorHandler(message_in_q=door_lock_in_queue, message_out_q=door_lock_out_queue, serial_q=serial_in_queue)
  mqtt_door_lock_send_thread = MqttSender(req_q=door_lock_out_queue, host=mqtt_broker_host, topic=mqtt_door_lock_topic)

  try:
    # HID code handling thread
    hid_thread.setDaemon(True)
    hid_thread.start()
    logging.info("HID handling thread has started")

    # Serial handling thread
    ser_thread.setDaemon(True)
    ser_thread.start()
    logging.info("Serial handling thread has started")

    # Fake serial handling thread
    fake_ser_thread.setDaemon(True)
    fake_ser_thread.start()
    logging.info("Fake serial handling thread has started")

    # MQTT thread (for sending HID codes)
    mqtt_send_thread.setDaemon(True)
    mqtt_send_thread.start()
    logging.info("HID sensor MQTT send thread started")

    # MQTT thread (for receiving door open messages)
    mqtt_rec_thread.setDaemon(True)
    mqtt_rec_thread.start()
    logging.info("Door lock MQTT rx thread started")

    # door lock thread for managing unlock and lock
    door_handler.setDaemon(True)
    door_handler.start()
    logging.info("Door lock handler thread started")

    # MQTT thread (for sending door close messages)
    mqtt_door_lock_send_thread.setDaemon(True)
    mqtt_door_lock_send_thread.start()
    logging.info("Door lock MQTT tx thread started")

    # keep waiting
    while(True):
      time.sleep(5)
      logging.debug("Main thread running")

  except (KeyboardInterrupt, SystemExit):
    logging.info("Cleaning up... killing threads")
    ser_thread.join(3)
    fake_ser_thread.join(3)
    hid_thread.join(3)
    mqtt_send_thread.join(3)
    door_handler.join(3)
    mqtt_door_lock_send_thread.join(3)
    sys.exit()

if __name__ == '__main__':
  logging.info("Main method, starting")
  run_program()