import os
import socket
import struct
from dataclasses import dataclass
from typing import Callable, Dict, Optional

from influxdb_client_3 import InfluxDBClient3, InfluxDBError, Point

print("Python server starting...", flush=True)

# =========================
# Configuration
# =========================

@dataclass(frozen=True)
class UdpConfig:
    host: str = "0.0.0.0"
    port: int = 8080
    buffer_size: int = 2048


@dataclass(frozen=True)
class InfluxConfig:
    host: str
    token: str
    database: str
    port: int = 8181

    @staticmethod
    def from_env() -> "InfluxConfig":
        host = os.getenv("INFLUX_HOST", "http://localhost")
        token = os.getenv("INFLUX_TOKEN")
        database = os.getenv("INFLUX_DATABASE")

        if not token:
            raise RuntimeError("INFLUX_TOKEN is not set")
        if not database:
            raise RuntimeError("INFLUX_DATABASE is not set")

        return InfluxConfig(
            host=host,
            token=token,
            database=database,
            port=int(os.getenv("INFLUX_PORT", "8181")),
        )


# =========================
# Data Models
# =========================

@dataclass(frozen=True)
class DecodedMessage:
    message_type: int
    message_id: int
    length: int
    payload: bytes


# =========================
# Decoder
# =========================

class PacketDecoder:
    MIN_PACKET_SIZE = 9

    @staticmethod
    def decode(data: bytes) -> DecodedMessage:
        if len(data) < PacketDecoder.MIN_PACKET_SIZE:
            raise ValueError(f"Packet too short: got {len(data)} bytes")

        message_type = data[0]
        message_id = struct.unpack("<I", data[4:8])[0]
        length = data[8]

        if len(data) < 9 + length:
            raise ValueError(
                f"Packet payload incomplete: expected {length} payload bytes, "
                f"but packet length is only {len(data)}"
            )

        payload = data[9:9 + length]

        return DecodedMessage(
            message_type=message_type,
            message_id=message_id,
            length=length,
            payload=payload,
        )

    @staticmethod
    def decode_temperature(msg: DecodedMessage) -> float:
        if len(msg.payload) < 2:
            raise ValueError("Temperature payload too short: need at least 2 bytes")

        temp_raw = struct.unpack("<h", msg.payload[0:2])[0]
        return temp_raw / 16.0


# =========================
# Influx Writer
# =========================

class InfluxWriter:
    def __init__(self, config: InfluxConfig) -> None:
        self._config = config
        self._client = InfluxDBClient3(
            host=config.host,
            token=config.token,
            database=config.database,
            port=config.port,
        )

    def close(self) -> None:
        self._client.close()

    def write_temperature(self, msg: DecodedMessage, temperature_c: float) -> None:
        point = (
            Point("temperature")
            .tag("message_id", f"0x{msg.message_id:08X}")
            .field("value_c", temperature_c)
        )

        self._client.write(point)
        print(
            f"[InfluxDB] Wrote temperature: id=0x{msg.message_id:08X}, "
            f"value={temperature_c:.2f} C"
        )


# =========================
# UDP Server
# =========================

class UdpServer:
    def __init__(self, config: UdpConfig) -> None:
        self._config = config
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._sock.bind((config.host, config.port))

    def recv(self) -> tuple[bytes, tuple[str, int]]:
        return self._sock.recvfrom(self._config.buffer_size)

    def close(self) -> None:
        self._sock.close()

    def log_start(self) -> None:
        print(f"Listening on UDP {self._config.host}:{self._config.port}")


# =========================
# Application
# =========================

class TelemetryApp:
    TEMPERATURE_MESSAGE_ID = 0x00000001

    def __init__(self, udp_server: UdpServer, influx_writer: InfluxWriter) -> None:
        self._udp_server = udp_server
        self._influx_writer = influx_writer
        self._handlers: Dict[int, Callable[[DecodedMessage], None]] = {
            self.TEMPERATURE_MESSAGE_ID: self._handle_temperature,
        }

    def run(self) -> None:
        self._udp_server.log_start()

        while True:
            try:
                data, addr = self._udp_server.recv()
                msg = PacketDecoder.decode(data)

                print(
                    f"[UDP] {addr} "
                    f"type={msg.message_type} "
                    f"id=0x{msg.message_id:08X} "
                    f"len={msg.length} "
                    f"payload={msg.payload.hex(' ')}"
                )

                handler = self._handlers.get(msg.message_id)
                if handler is None:
                    print(f"[INFO] No handler for message id 0x{msg.message_id:08X}")
                    continue

                handler(msg)

            except ValueError as e:
                print(f"[Decode Error] {e}")
            except InfluxDBError as e:
                print(f"[InfluxDB Error] {e}")
            except KeyboardInterrupt:
                print("\nShutting down...")
                break
            except Exception as e:
                print(f"[Unhandled Error] {e}")

    def _handle_temperature(self, msg: DecodedMessage) -> None:
        temperature_c = PacketDecoder.decode_temperature(msg)
        print(f"[Telemetry] Temperature: {temperature_c:.2f} C")
        self._influx_writer.write_temperature(msg, temperature_c)


# =========================
# Entry Point
# =========================

def main() -> None:
    udp_config = UdpConfig()
    influx_config = InfluxConfig.from_env()

    udp_server = UdpServer(udp_config)
    influx_writer = InfluxWriter(influx_config)

    try:
        app = TelemetryApp(udp_server, influx_writer)
        app.run()
    finally:
        udp_server.close()
        influx_writer.close()


if __name__ == "__main__":
    main()