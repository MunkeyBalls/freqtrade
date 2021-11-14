# pragma pylint: disable=unused-argument, unused-variable, protected-access, invalid-name

"""
This module manage MQTT communication
"""
import json
import logging
import re
import pickle
from datetime import date, datetime, timedelta
from html import escape
from itertools import chain
from math import isnan
from typing import Any, Callable, Dict, List, Optional, Union

from freqtrade.__init__ import __version__
from freqtrade.constants import DUST_PER_COIN
from freqtrade.enums import RPCMessageType
from freqtrade.exceptions import OperationalException
from freqtrade.misc import chunks, plural, round_coin_value
from freqtrade.persistence import Trade
from freqtrade.rpc import RPC, RPCException, RPCHandler

import paho.mqtt.client as mqtt

from timeit import default_timer as timer
import time

logger = logging.getLogger(__name__)
logger.debug('Included module rpc.mqtt ...')

class Mqtt(RPCHandler):
    """  This class handles all MQTT communication """

    def __init__(self, rpc: RPC, config: Dict[str, Any]) -> None:
        """
        Init the MQTT call, and init the super class RPCHandler
        :param rpc: instance of RPC Helper class
        :param config: Configuration object
        :return: None
        """
        super().__init__(rpc, config)

        self._init()

    def on_connect(self, client, userdata, flags, rc):
        logger.info("Connected to MQTT broker")
        client.connected_flag=True

    def on_disconnect(self, client, userdata, rc):
        logger.warning("Disconnected from MQTT broker")
        client.connected_flag=False

    def on_message(self, client, userdata, message):
        #global messages
        logger.warning("Received MQTT: %s on topic: %s", message.payload, message.topic)
        # TODO: Run commands based on topic / payload
        #m = str(message.payload.decode("utf-8"))
        # messages.append(m)#put messages in list
        # q.put(m) #put messages on queue

    def _init(self) -> None:
        try:
            if self._config.get('mqtt', {}).get('enabled', False):
                logger.info("Connecting to MQTT broker")
                hostname = self._config["mqtt"]["ip"]
                port = self._config["mqtt"]["port"]                
                self.mqttc = mqtt.Client(self._config["bot_name"] + str(time.time()))
                if self._config.get('mqtt', {}).get('username', False):
                    self.mqttc.username_pw_set(self._config["mqtt"]["username"], self._config["mqtt"]["password"])
                self.mqttc.connected_flag = False
                self.mqttc.on_connect=self.on_connect
                self.mqttc.on_disconnect=self.on_disconnect
                self.mqttc.on_message=self.on_message
                self.mqttc.connect(hostname, port)                
                self.mqttc.loop_start()
                topic = self._config["mqtt"]["topic"] + "/command/#"
                logger.warning("Subscribing to %s", topic)
                subscription=self.mqttc.subscribe(topic, 0)
        except Exception as e: 
            logger.warning("Unable to connect to MQTT broker")
            logger.warning("MQTT Exception: %s", str(e))

    def _send_mqtt(self, topic: str, msg: str):        
        if self.mqttc != None and self.mqttc.connected_flag == True:
            try:
                combined_topic = self._config["mqtt"]["topic"] + "/" + topic
                self.mqttc.publish(combined_topic, msg)
            except:
                logger.warning("Unable to publish MQTT msg")
        else:
            logger.warning("MQTT not connected")

    def cleanup(self) -> None:
        """
        Stops all running MQTT threads.
        :return: None
        """
        if hasattr(self, 'mqttc'):
            logger.warning("Cleanup MQTT rpc module..")
            self.mqttc.loop_stop() 
            self.mqttc.disconnect() 
   

    def send_msg(self, msg: Dict[str, Any]) -> None:
        """ Send a message to MQTT topic  """

        msg_type = msg['type']       

        # TODO: Compose json message based on type with relevant info
        #message = self.compose_message(msg, msg_type)

        if msg_type == RPCMessageType.BUY:            
            self._send_mqtt(str(msg_type), msg['pair'])
        elif msg_type == RPCMessageType.BUY_FILL:            
            self._send_mqtt(str(msg_type), msg['pair'])

        elif msg_type == RPCMessageType.SELL:
            self._send_mqtt(str(msg_type), msg['pair'])
        elif msg_type == RPCMessageType.SELL_FILL:
            self._send_mqtt(str(msg_type), msg['pair'])

        elif msg_type in (RPCMessageType.BUY_CANCEL, RPCMessageType.SELL_CANCEL):
            self._send_mqtt(str(msg_type), msg['pair'])


    def _send_msg(self, msg: str) -> None:
        """
        Send given markdown message
        :param msg: message
        :param bot: alternative bot
        :param parse_mode: telegram parse mode
        :return: None
        """

    def _autodiscover(self):
        bot_name = self._config["bot_name"]
        topic = "homeassistant" # TODO: Autodiscovery port
        self.mqttc.publish(topic, '')
