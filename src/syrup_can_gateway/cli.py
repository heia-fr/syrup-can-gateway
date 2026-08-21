# SPDX-FileCopyrightText: 2026 Jacques Supcik <jacques.supci@hefr.ch>
#
# SPDX-License-Identifier: MIT

import asyncio
import sys
from typing import NamedTuple
from urllib.parse import urlsplit

import aiomqtt
import can
from loguru import logger
from typer import Option, Typer

from syrup_can_gateway import SyrupCanGateway

CAN_BITRATE = 500000

app = Typer()


class MqttConfig(NamedTuple):
    url: str
    base_topic: str
    username: str | None
    password: str | None


class CanConfig(NamedTuple):
    interface: str
    channel: str


async def daemon(
    mqtt_config: MqttConfig,
    can_config: CanConfig,
):
    o = urlsplit(mqtt_config.url)
    if o.scheme not in ("mqtt", "mqtts"):
        logger.error(f"Invalid MQTT URL: {mqtt_config.url}")
        return

    assert o.hostname is not None

    hostname = o.hostname
    port = o.port or (8883 if o.scheme == "mqtts" else 1883)

    with can.Bus(
        channel=can_config.channel,
        interface=can_config.interface,
        bitrate=CAN_BITRATE,
    ) as bus:
        while True:
            try:
                logger.info(f"Connecting to MQTT broker at {hostname}:{port}")
                async with aiomqtt.Client(
                    hostname=hostname,
                    port=port,
                    username=mqtt_config.username,
                    password=mqtt_config.password,
                ) as client:
                    logger.debug(f"Subscribing to topic {mqtt_config.base_topic}/#")
                    await client.subscribe(f"{mqtt_config.base_topic}/#", qos=1)
                    logger.info(f"Base topic: {mqtt_config.base_topic}")

                    gateway = SyrupCanGateway(
                        bus=bus,
                        mqtt_client=client,
                        mqtt_base_topic=mqtt_config.base_topic,
                    )
                    await gateway.run()
            except* aiomqtt.MqttError as errors:
                logger.warning(f"MQTT connection lost: {errors}; retrying in 5 seconds")
            await asyncio.sleep(5)

    logger.info("Exiting controller")


@app.command()
def main(  # noqa
    debug: bool = Option(False, help="Enable debug logging", envvar="SYRUP_DEBUG"),
    quiet: bool = Option(False, help="Enable quiet logging", envvar="SYRUP_QUIET"),
    mqtt_url: str = Option(
        "mqtt://mqtt.local:1883",
        help="MQTT broker URL",
        envvar="SYRUP_MQTT_URL",
    ),
    mqtt_base_topic: str = Option(
        "heiafr/ms", help="MQTT base topic", envvar="SYRUP_MQTT_BASE_TOPIC"
    ),
    mqtt_username: str = Option(
        None, help="MQTT username", envvar="SYRUP_MQTT_USERNAME"
    ),
    mqtt_password: str = Option(
        None, help="MQTT password", envvar="SYRUP_MQTT_PASSWORD"
    ),
    can_interface: str = Option(
        "socketcan", help="CAN interface", envvar="SYRUP_CAN_INTERFACE"
    ),
    can_channel: str = Option("can0", help="CAN channel", envvar="SYRUP_CAN_CHANNEL"),
):
    """
    SYRUP CAN GATEWAY

    Copyright (c) 2026 Jacques Supcik, HEIA-FR
    """
    logger.remove()
    if debug:
        logger.add(sys.stderr, level="DEBUG")
    elif quiet:
        logger.add(sys.stderr, level="WARNING")
    else:
        logger.add(sys.stderr, level="INFO")

    try:
        asyncio.run(
            daemon(
                MqttConfig(
                    url=mqtt_url,
                    base_topic=mqtt_base_topic,
                    username=mqtt_username,
                    password=mqtt_password,
                ),
                CanConfig(
                    interface=can_interface,
                    channel=can_channel,
                ),
            )
        )
    except KeyboardInterrupt:
        logger.info("Exiting...")


if __name__ == "__main__":
    app()
