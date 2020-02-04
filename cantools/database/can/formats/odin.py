# Load a CAN database in Odin (Tesla) format.

import sys
import logging
from collections import defaultdict
from decimal import Decimal

import json

from ..signal import Signal
from ..signal import Decimal as SignalDecimal
from ..message import Message
from ..node import Node
from ..bus import Bus
from ..internal_database import InternalDatabase
from ...utils import start_bit
from .utils import num


LOGGER = logging.getLogger(__name__)

def _get_value(element, key, default):
    if key in element:
        return element[key]
    else:
        return default

def _start_bit(offset, byte_order):
    if byte_order == 'big_endian':
        return (8 * (offset // 8) + (7 - (offset % 8)))
    else:
        return offset

def _load_signal(signal_json, name):
    """Load given signal element and return a signal object.

    """

    # Default values.
    is_float = False
    labels = None
    notes = None
    intercept = 0 
    slope = 1
    decimal = SignalDecimal(Decimal(slope), Decimal(intercept))

    length = int(_get_value(signal_json, 'width', 1))
    offset = _get_value(signal_json, 'start_position', None)
    offset = (int(offset) if offset is not None else None)
    is_signed = True if _get_value(signal_json, 'signedness', "UNSIGNED") == 'SIGNED' else False
    byte_order = ('big_endian' if _get_value(signal_json, 'endianness', "LITTLE") == 'BIG'
                  else 'little_endian')
    minimum = _get_value(signal_json, 'minimum', None)
    minimum = int(minimum) if minimum is not None else None
    maximum = _get_value(signal_json, 'maximum', None)
    maximum = int(maximum) if maximum is not None else None
    receivers = _get_value(signal_json, 'receivers', [])
    slope = float(_get_value(signal_json, 'scale', slope))
    unit = _get_value(signal_json, 'units', None) 

    decimal.scale = slope
    intercept = float(_get_value(signal_json, 'offset', 0))
    decimal.offset = intercept

    value_description = _get_value(signal_json, 'value_description', None)
    if value_description:
        labels = {}
        for k, v in value_description.items():
            labels[v] = k

    signal = Signal(name=name,
                    start=_start_bit(offset, byte_order),
                    length=length,
                    receivers=receivers,
                    byte_order=byte_order,
                    is_signed=is_signed,
                    scale=slope,
                    offset=intercept,
                    minimum=minimum,
                    maximum=maximum,
                    unit=unit,
                    choices=labels,
                    comment=notes,
                    is_float=is_float,
                    decimal=decimal)
    mux_id = _get_value(signal_json, 'mux_id', None)
    if mux_id is not None:
        signal.multiplexer_ids = [int(mux_id)]
        signal.is_multiplexed = True
    is_muxer = _get_value(signal_json, 'is_muxer', None)
    if is_muxer is not None and is_muxer == True:
        signal.is_multiplexer = True

    return signal
 
def _parse_signals(signals_json):
    signals = []
    for name, signal in signals_json.items():
        new_signal = _load_signal(signal, name)
        if new_signal is not None:
            signals.append(new_signal)
    return signals

def _load_message(message, name, bus_name, strict):
    """Load given message and return a message object.

    """
    frame_id = int(_get_value(message, 'message_id', 0))
    is_extended_frame = False
    notes = None
    length = _get_value(message, 'length_bytes', 'auto')
    cycle_time = _get_value(message, 'cycle_time', None)
    cycle_time = int(cycle_time) if cycle_time is not None else None
    senders = _get_value(message, 'senders', [])
    send_type = _get_value(message, 'send_type', None)

    signals = _parse_signals(_get_value(message, 'signals', {}))

    # find and assign multiplexer_signal
    multiplexer = None
    for signal in signals:
        if signal.is_multiplexer:
            multiplexer = signal.name
    if multiplexer:
        for signal in signals:
            if signal.multiplexer_ids and len(signal.multiplexer_ids) > 0:
                signal.multiplexer_signal = multiplexer
    
    if length == 'auto':
        if signals:
            last_signal = sorted(signals, key=start_bit)[-1]
            length = (start_bit(last_signal) + last_signal.length + 7) // 8
        else:
            length = 0
    else:
        length = int(length)

    return Message(frame_id=frame_id,
                   is_extended_frame=is_extended_frame,
                   name=name,
                   length=length,
                   senders=senders,
                   send_type=send_type,
                   cycle_time=cycle_time,
                   signals=signals,
                   comment=notes,
                   bus_name=bus_name,
                   strict=strict)

def load_string(string, strict):
    """Parse given Odin format string.

    """

    # Gets the root of the Odin JSON
    try:
        odin = json.loads(string)
        messages_json = odin['messages']
        bus_json = odin['busMetadata']
    except KeyError:
        raise ValueError(
                'Expected "messages" and "busMetadata" at root of the Odin JSON file'
                )

    nodes = set([])
    bus = None
    if 'name' in messages_json:
        bus = messages_json['name']
    messages = []
    if bus:
        buses = [bus]
    else:
        buses = []

    for message_name, message in messages_json.items():
        new_message = _load_message(message, message_name, bus, strict)
        nodes.update(new_message.senders)
        for signal in new_message.signals:
            nodes.update(signal.receivers)
        messages.append(new_message)

    nodes = list(nodes)

    return InternalDatabase(messages, 
                            [
                                Node(name=node, comment=None)
                                for node in nodes
                            ],
                            buses,
                            "0")


    



