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
        logger.warning("Received MQTT: %s on topic: %s", message.payload, message.topic)
        # results = self._rpc._rpc_trade_status()
        # s = json.dumps(results)
        # self._send_mqtt('sensor', s)
        #self._loop()
        # TODO: Run commands based on topic / payload
        # m = str(message.payload.decode("utf-8"))
        # messages.append(m)#put messages in list
        # q.put(m) #put messages on queue

    def _init(self) -> None:
        try:
            if self._config.get('mqtt', {}).get('enabled', False):
                logger.info("Connecting to MQTT broker")
                self.bot_name = self._config["bot_name"]
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
        if self.mqttc != None and self.mqttc.connected_flag == True:
            message = self.compose_message(msg, msg['type'])
            if message:
                self._send_mqtt(msg['type'], message)      


    def compose_message(self, msg: Dict[str, Any], msg_type: RPCMessageType) -> str:
        if msg_type in [RPCMessageType.ENTRY, RPCMessageType.ENTRY_FILL]:
            message = self._format_entry_msg(msg)

        elif msg_type in [RPCMessageType.EXIT, RPCMessageType.EXIT_FILL]:
            message = self._format_exit_msg(msg)

        elif msg_type in (RPCMessageType.ENTRY_CANCEL, RPCMessageType.EXIT_CANCEL):
            msg['message_side'] = 'enter' if msg_type == RPCMessageType.ENTRY_CANCEL else 'exit'
            msg['type'] = 'entry_cancel' if msg_type == RPCMessageType.ENTRY_CANCEL else 'exit_cancel'
            message = ("\N{WARNING SIGN} *{exchange}:* "
                       "Cancelling open {message_side} Order for {pair} (#{trade_id}). "
                       "Reason: {reason}.".format(**msg))

        elif msg_type == RPCMessageType.ENTRY_CANCEL_STRATEGY:
            msg['type'] = 'entry_cancel_strategy'
            msg['message_side'] = 'enter'
            message = ("\N{WARNING SIGN} *{exchange}:* "
                       "Cancelling open {message_side} Order for {pair} (#{trade_id}). "
                       "Reason: {reason}.".format(**msg))

        elif msg_type in [RPCMessageType.EXIT_HOLD]:
            message = self._format_exit_hold_msg(msg)

        else:
            return None
        return message 


    def _autodiscover(self):
        topic = "homeassistant" # TODO: Autodiscovery port
        self.mqttc.publish(topic, '')

    def _format_trade_status_msg(self):
        results = self._rpc._rpc_trade_status()
        message = json.dumps(results)        
        return message

    def _format_entry_msg(self, msg: Dict[str, Any]) -> str:
        is_fill = msg['type'] == RPCMessageType.ENTRY_FILL
        if self._rpc._fiat_converter:
            msg['stake_amount_fiat'] = self._rpc._fiat_converter.convert_amount(
            msg['stake_amount'], msg['stake_currency'], msg['fiat_currency'])
        else:
            msg['stake_amount_fiat'] = 0

        if is_fill:
            open_rate = msg['open_rate']
            type = 'entry_fill'            
        else:
            open_rate = msg['limit']
            type = 'entry'

        msg['type'] = type

        message = {
            'trade_id': msg['trade_id'],
            'type': type,
            'entry_tag': msg['enter_tag'],
            'exchange': msg['exchange'],
            'pair': msg['pair'],
            'open_rate': open_rate,
            #'curren_rate': open_rate,
            'stake_amount': msg['stake_amount'],
            'amount': msg['amount'],
            'bot_name': self.bot_name
        }

        return json.dumps(message)

    def _format_exit_msg(self, msg: Dict[str, Any]) -> str:
        is_fill = msg['type'] == RPCMessageType.EXIT_FILL
        msg['amount'] = round(msg['amount'], 8)
        msg['profit_percent'] = round(msg['profit_ratio'] * 100, 2)
        msg['duration'] = msg['close_date'].replace(microsecond=0) - msg['open_date'].replace(microsecond=0)
        msg['duration_min'] = msg['duration'].total_seconds() / 60
        msg['enter_tag'] = msg['enter_tag'] if "enter_tag" in msg.keys() else None
        msg['min_ratio'] = round(msg['min_ratio'] * 100., 2)
        msg['max_ratio'] = round(msg['max_ratio'] * 100, 2)
        msg['type'] = 'exit'

        if is_fill:
            type = 'exit_fill'
        else:
            type = 'exit'        

        msg['type'] = type

        mesage = {
            'type': type,
            'trade_id': msg['trade_id'],
            'exchange': msg['exchange'],
            'pair': msg['pair'],
            'gain': msg['gain'],
            'limit': msg['limit'],
            'order_type': msg['order_type'],
            'amount': msg['amount'],
            # 'min_rate': trade.min_rate,
            # 'min_ratio': min_ratio,
            # 'max_rate': trade.max_rate,
            # 'max_ratio': max_ratio,
            # 'open_rate': trade.open_rate,
            'close_rate': msg['close_rate'],
            # 'current_rate': current_rate,
            'profit_amount': msg['profit_amount'],
            'profit_ratio': msg['profit_ratio'],
            'bot_name': self.bot_name
        }

        return json.dumps(mesage)      


    def _format_exit_hold_msg(self, msg: Dict[str, Any]) -> str:
        msg['type'] = 'exit_hold'
        msg['bot_name'] = self.bot_name
        return json.dumps(msg)