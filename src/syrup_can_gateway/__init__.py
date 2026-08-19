# SPDX-FileCopyrightText: 2026 Jacques Supcik <jacques.supci@hefr.ch>
#
# SPDX-License-Identifier: MIT

import asyncio
import json
import math
import re
import struct
from dataclasses import dataclass

import can
from aiomqtt import Client as MQTTClient
from can.notifier import MessageRecipient
from loguru import logger

WHEEL_SIZE = 0.7  # meter

CAN_CMD_BIKE_SPEED = 0x00 << 3
CAN_CMD_BIKE_STATUS = 0x01 << 3
CAN_CMD_BIKE_BREAK = 0x02 << 3
CAN_CMD_WATER_SCALE = 0x08 << 3

CAN_ID_RESET_SPEEDOMETER = 0x100
CAN_ID_SET_BREAK_POSITION = 0x148
CAN_ID_SET_BREAK_TRIM = 0x150
CAN_ID_BREAK_CALIBRATE = 0x160

DEVICE_ID_MASK = 0x07
CMD_MASK = 0xF8


@dataclass
class SyrupCanGateway:
    bus: can.BusABC
    mqtt_client: MQTTClient
    mqtt_base_topic: str

    def _handle_speedometer_reset_message(self, bike_id: int):
        msg = can.Message(
            arbitration_id=CAN_ID_RESET_SPEEDOMETER | bike_id,
            data=[],
            is_extended_id=False,
        )
        self.bus.send(msg)

    def _handle_break_set_message(self, bike_id: int, target_pos: int):
        msg = can.Message(
            arbitration_id=CAN_ID_SET_BREAK_POSITION | bike_id,
            data=struct.pack("<L", target_pos),
            is_extended_id=False,
        )
        self.bus.send(msg)

    def _handle_break_trim_message(self, bike_id: int, trim_value: int):
        msg = can.Message(
            arbitration_id=CAN_ID_SET_BREAK_TRIM | bike_id,
            data=struct.pack("<L", trim_value),
            is_extended_id=False,
        )
        self.bus.send(msg)

    def _handle_break_calibrate_message(self, bike_id: int):
        msg = can.Message(
            arbitration_id=CAN_ID_BREAK_CALIBRATE | bike_id,
            data=[],
            is_extended_id=False,
        )
        self.bus.send(msg)

    async def mqtt_task(self):
        logger.info("Starting MQTT task")
        async for message in self.mqtt_client.messages:
            topic = str(message.topic)
            payload = message.payload.decode()
            if m := re.search(r"/speedometer/(\d+)/reset$", topic):
                bike_id = int(m.group(1))
                self._handle_speedometer_reset_message(bike_id)
            elif topic.endswith("/speedometer/reset"):
                self._handle_speedometer_reset_message(0)
            if m := re.search(r"/break/(\d+)/set$", topic):
                bike_id = int(m.group(1))
                target_pos = int(payload)
                self._handle_break_set_message(bike_id, target_pos)
            elif topic.endswith("/break/set"):
                target_pos = int(payload)
                self._handle_break_set_message(0, target_pos)
            elif m := re.search(r"/break/(\d+)/trim$", topic):
                bike_id = int(m.group(1))
                trim_value = int(payload)
                self._handle_break_trim_message(bike_id, trim_value)
            elif m := re.search(r"/break/(\d+)/calibrate$", topic):
                bike_id = int(m.group(1))
                self._handle_break_calibrate_message(bike_id)
            elif topic.endswith("/break/calibrate"):
                self._handle_break_calibrate_message(0)

    # ---- CAN message handlers ----

    async def _handle_bike_speed_message(self, msg: can.Message):
        bike_id = msg.arbitration_id & DEVICE_ID_MASK
        dt, rotations = struct.unpack("<LL", msg.data)
        speed = 0 if dt == 0 else 3600 * 1000 * WHEEL_SIZE * math.pi / dt

        await self.mqtt_client.publish(
            f"{self.mqtt_base_topic}/bike/{bike_id}/speed",
            json.dumps({"speed": speed, "rotations": rotations}),
            qos=0,
        )

    async def _handle_bike_status_message(self, msg: can.Message):
        bike_id = msg.arbitration_id & DEVICE_ID_MASK
        dt, rotations = struct.unpack("<LL", msg.data)
        speed = 0 if dt == 0 else 3600 * 1000 * WHEEL_SIZE * math.pi / dt

        await self.mqtt_client.publish(
            f"{self.mqtt_base_topic}/bike/{bike_id}/status",
            json.dumps({"speed": speed, "rotations": rotations}),
            qos=0,
        )

    async def _handle_bike_break_message(self, msg: can.Message):
        bike_id = msg.arbitration_id & DEVICE_ID_MASK
        current_pos, target_pos = struct.unpack("<LL", msg.data)

        await self.mqtt_client.publish(
            f"{self.mqtt_base_topic}/bike/{bike_id}/break-status",
            json.dumps({"current_pos": current_pos, "target_pos": target_pos}),
            qos=0,
        )

    async def _handle_water_scale_message(self, msg: can.Message):
        level = struct.unpack("<L", msg.data)

        await self.mqtt_client.publish(
            f"{self.mqtt_base_topic}/water/level",
            json.dumps({"level": level}),
            qos=0,
        )

    async def can_task(self):
        logger.info("Starting CAN task")
        reader = can.AsyncBufferedReader()
        listeners: list[MessageRecipient] = [
            reader,
        ]
        with can.Notifier(self.bus, listeners, loop=asyncio.get_running_loop()):
            while True:
                msg = await reader.get_message()
                logger.debug(f"Received CAN message: {msg}")
                cmd = msg.arbitration_id & CMD_MASK

                if cmd == CAN_CMD_BIKE_SPEED:
                    await self._handle_bike_speed_message(msg)
                elif cmd == CAN_CMD_BIKE_STATUS:
                    await self._handle_bike_status_message(msg)
                elif cmd == CAN_CMD_BIKE_BREAK:
                    await self._handle_bike_break_message(msg)
                elif cmd == CAN_CMD_WATER_SCALE:
                    await self._handle_water_scale_message(msg)

    # --- Main run loop ---

    async def run(self):
        logger.info("Starting yrupCanGateway tasks")
        async with asyncio.TaskGroup() as tg:
            tg.create_task(self.mqtt_task())
            tg.create_task(self.can_task())
        logger.info("SyrupCanGateway tasks completed")
