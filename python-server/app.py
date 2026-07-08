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
    longtitude: float = 0.0
    latitude: float = 0.0

# =========================
# Decoder
# =========================

class PacketDecoder:
    MIN_PACKET_SIZE = 6
    TYPE_NOTECARD_CAN = 1
    TYPE_WIFI_TEXT = 0
    
    @staticmethod
    def decode(data: bytes) -> DecodedMessage:
        if len(data) < PacketDecoder.HEADER_SIZE:
            raise ValueError(f"Packet too short: got {len(data)} bytes")

        message_type = data[0]
        message_id = struct.unpack("<I", data[1:5])[0]
        length = data[5]

        payload_start = PacketDecoder.HEADER_SIZE

        longitude = 0.0
        latitude = 0.0

        if message_type == PacketDecoder.TYPE_NOTECARD_CAN:
            payload_end = payload_start + length

            if len(data) < payload_end:
                raise ValueError(
                    f"Packet payload incomplete: expected {length} payload bytes, "
                    f"but packet length is only {len(data)}"
                )

            payload = data[payload_start:payload_end]

            if len(data) >= payload_end + 8:
                longitude, latitude = struct.unpack(
                    "<ff",
                    data[payload_end:payload_end + 8]
                )

        elif message_type == PacketDecoder.TYPE_WIFI_TEXT:
            raw_payload = data[payload_start:]

            if length <= len(raw_payload):
                payload = raw_payload[:length]
            else:
                payload = raw_payload

            payload = payload.rstrip(b" \x00\r\n")
            length = len(payload)

        else:
            raise ValueError(f"Unknown message_type: {message_type}")

        return DecodedMessage(
            message_type=message_type,
            message_id=message_id,
            length=len(payload),
            payload=payload,
            longtitude=longitude,
            latitude=latitude
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
            .tag("message_type", "wifi" if msg.message_type == 0 else "cellular")
            .field("value", temperature_c)
        )

        self._client.write(point)

        print(
            f"[InfluxDB] Wrote temperature: id=0x{msg.message_id:08X}, "
            f"value={temperature_c:.2f} C"
        )

    def write_soc(self, msg: DecodedMessage, soc_percent: float) -> None:
        point = (
            Point("battery_soc")
            .tag("message_id", f"0x{msg.message_id:08X}")
            .tag("message_type", "wifi" if msg.message_type == 0 else "cellular")
            .field("value", soc_percent)
        )

        self._client.write(point)

        print(
            f"[InfluxDB] Wrote battery state of charge: id=0x{msg.message_id:08X}, "
            f"value={soc_percent:.2f} %"
        )

    def write_bat_avg_temp(self, msg: DecodedMessage, avg_temp_c: float) -> None:
        point = (
            Point("battery_avg_temp")
            .tag("message_id", f"0x{msg.message_id:08X}")
            .tag("message_type", "wifi" if msg.message_type == 0 else "cellular")
            .field("value", avg_temp_c)
        )

        self._client.write(point)

        print(
            f"[InfluxDB] Wrote battery average temperature: id=0x{msg.message_id:08X}, "
            f"value={avg_temp_c:.2f} C"
        )

    def write_bat_max_temp(self, msg: DecodedMessage, max_temp_c: float) -> None:
        point = (
            Point("battery_max_temp")
            .tag("message_id", f"0x{msg.message_id:08X}")
            .tag("message_type", "wifi" if msg.message_type == 0 else "cellular")
            .field("value", max_temp_c)
        )

        self._client.write(point)

        print(
            f"[InfluxDB] Wrote battery max temperature: id=0x{msg.message_id:08X}, "
            f"value={max_temp_c:.2f} C"
        )

    def write_bat_pack_voltage(self, msg: DecodedMessage, voltage_v: float) -> None:
        point = (
            Point("battery_pack_voltage")
            .tag("message_id", f"0x{msg.message_id:08X}")
            .tag("message_type", "wifi" if msg.message_type == 0 else "cellular")
            .field("value", voltage_v)
        )

        self._client.write(point)

        print(
            f"[InfluxDB] Wrote battery pack voltage: id=0x{msg.message_id:08X}, "
            f"value={voltage_v:.2f} V"
        )

    def write_bat_pack_current(self, msg: DecodedMessage, current_a: float) -> None:
        point = (
            Point("battery_pack_current")
            .tag("message_id", f"0x{msg.message_id:08X}")
            .tag("message_type", "wifi" if msg.message_type == 0 else "cellular")
            .field("value", current_a)
        )

        self._client.write(point)

        print(
            f"[InfluxDB] Wrote battery pack current: id=0x{msg.message_id:08X}, "
            f"value={current_a:.2f} A"
        )

    def write_bat_vol_module(self, msg: DecodedMessage, module_num: int, voltage_v: float) -> None:
        point = (
            Point("battery_vol_module")
            .tag("message_id", f"0x{msg.message_id:08X}")
            .tag("message_type", "wifi" if msg.message_type == 0 else "cellular")
            .field("module", module_num)
            .field("value", voltage_v)
        )

        self._client.write(point)

        print(
            f"[InfluxDB] Wrote battery voltage module {module_num}: id=0x{msg.message_id:08X}, "
            f"value={voltage_v:.2f} V"
        )

    def write_bat_hv_fault(self, msg: DecodedMessage, fault_flags: int) -> None:
        point = (
            Point("battery_hv_fault")
            .tag("message_id", f"0x{msg.message_id:08X}")
            .tag("message_type", "wifi" if msg.message_type == 0 else "cellular")
            .field("value", fault_flags)
        )

        self._client.write(point)

        print(
            f"[InfluxDB] Wrote battery HV fault flags: id=0x{msg.message_id:08X}, "
            f"value={fault_flags}"
        )

    def write_bat_hv_main_contractor(self, msg: DecodedMessage, main_contractor: int) -> None:
        point = (
            Point("battery_hv_main_contractor")
            .tag("message_id", f"0x{msg.message_id:08X}")
            .tag("message_type", "wifi" if msg.message_type == 0 else "cellular")
            .field("value", main_contractor)
        )

        self._client.write(point)

        print(
            f"[InfluxDB] Wrote battery HV main contactor status: id=0x{msg.message_id:08X}, "
            f"value={main_contractor}"
        )

    def write_bat_hv_motor_contractor(self, msg: DecodedMessage, motor_contractor: int) -> None:
        point = (
            Point("battery_hv_motor_contractor")
            .tag("message_id", f"0x{msg.message_id:08X}")
            .tag("message_type", "wifi" if msg.message_type == 0 else "cellular")
            .field("value", motor_contractor)
        )

        self._client.write(point)

        print(
            f"[InfluxDB] Wrote battery HV motor contactor status: id=0x{msg.message_id:08X}, "
            f"value={motor_contractor}"
        )

    def write_datapoint(self, point: Point):
        self._client.write(point)


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
    TELEMETRY_NODE_1_TEMP_1 = 0x11
    TELEMETRY_NODE_1_TEMP_2 = 0x12

    DRIVER_CTR_MAIN_HV_SW = 0x2000004
    DRIVER_CTR_MOTOR_HV_SW = 0x2000104
    DRIVER_CTR_MPPT_HV_SW = 0x2000204
    DRIVER_CTR_MOTOR_FR_SW = 0x2000304
    DRIVER_CTR_MOTOR_PE_SW = 0x2000404
    DRIVER_CTR_SERVICE_BR_SW = 0x4000504
    DRIVER_CTR_PARKING_BR_SW = 0x4000604
    DRIVER_CTR_RIGHT_TURN_SW = 0x4000704
    DRIVER_CTR_LEFT_TURN_SW = 0x4000804

    REAR_CTRL_MOTOR_RPM = 0x8000008
    REAR_CTRL_VEHICLE_SPEED = 0x4000108
    REAR_CTRL_ARR_VOL_1 = 0xA006408
    REAR_CTRL_ARR_CUR_1 = 0xA006508
    REAR_CTRL_BAT_MEAS_1 = 0xA006608
    REAR_CTRL_MPPT_TEMP_1 = 0xA006708
    REAR_CTRL_ARR_VOL_2 = 0xA00C808
    REAR_CTRL_ARR_CUR_2 = 0xA00C908
    REAR_CTRL_BAT_MEAS_2 = 0xA00CA08
    REAR_CTRL_MPPT_TEMP_2 = 0xA00CB08
    REAR_CTRL_ARR_VOL_3 = 0xA012C08
    REAR_CTRL_ARR_CUR_3 = 0xA012D08
    REAR_CTRL_BAT_MEAS_3 = 0xA012E08
    REAR_CTRL_MPPT_TEMP_3 = 0xA012F08
    REAR_CTRL_ARR_VOL_4 = 0xA019008
    REAR_CTRL_ARR_CUR_4 = 0xA019108
    REAR_CTRL_BAT_MEAS_4 = 0xA019208
    REAR_CTRL_MPPT_TEMP_4 = 0xA019308

    LV_BPS_BAT_VOL = 0x6000110
    LV_BPS_BAT_CUR = 0x6000210
    LV_BPS_SYS_CUR = 0x6000310
    LV_BPS_BAT_TEMP = 0x6000410

    HV_BPS_FAULT_FLAGS = 0x400010C
    HV_BPS_MAIN_CONTACTOR_STATUS = 0x800020C
    HV_BPS_MOTOR_CONTACTOR_STATUS = 0x800030C
    HV_BPS_MPPT_CONTACTOR_STATUS = 0x800040C
    HV_BPS_BAT_VOL = 0x600050C
    HV_BPS_BAT_CUR = 0x600060C
    HV_BPS_BAT_AVG_TEMP = 0x600070C
    HV_BPS_BAT_MAX_TEMP = 0x600080C
    HV_BPS_BAT_SOC = 0x600090C
    HV_BPS_MOD_VOL_1 = 0xC00640C
    HV_BPS_MOD_VOL_2 = 0xC00650C
    HV_BPS_MOD_VOL_3 = 0xC00660C
    HV_BPS_MOD_VOL_4 = 0xC00670C
    HV_BPS_MOD_VOL_5 = 0xC00680C
    HV_BPS_MOD_VOL_6 = 0xC00690C
    HV_BPS_MOD_VOL_7 = 0xC006A0C
    HV_BPS_MOD_VOL_8 = 0xC006B0C
    HV_BPS_MOD_VOL_9 = 0xC006C0C
    HV_BPS_MOD_VOL_10 = 0xC006D0C
    HV_BPS_MOD_VOL_11 = 0xC006E0C
    HV_BPS_MOD_VOL_12 = 0xC006F0C
    HV_BPS_MOD_VOL_13 = 0xC00700C
    HV_BPS_MOD_VOL_14 = 0xC00710C
    HV_BPS_MOD_VOL_15 = 0xC00720C
    HV_BPS_MOD_VOL_16 = 0xC00730C
    HV_BPS_MOD_VOL_17 = 0xC00740C
    HV_BPS_MOD_VOL_18 = 0xC00750C
    HV_BPS_MOD_VOL_19 = 0xC00760C
    HV_BPS_MOD_VOL_20 = 0xC00770C
    HV_BPS_MOD_VOL_21 = 0xC00780C
    HV_BPS_MOD_VOL_22 = 0xC00790C
    HV_BPS_MOD_VOL_23 = 0xC007A0C
    HV_BPS_MOD_VOL_24 = 0xC007B0C

    def __init__(self, udp_server: UdpServer, influx_writer: InfluxWriter) -> None:
        self._udp_server = udp_server
        self._influx_writer = influx_writer
        self._handlers: Dict[int, Callable[[DecodedMessage], None]] = {
            self.TELEMETRY_NODE_1_TEMP_1: self._handle_temperature,
            self.TELEMETRY_NODE_1_TEMP_2: self._handle_temperature,
            self.DRIVER_CTR_MAIN_HV_SW: self._handle_dr_ctrl_main_hv_sw,
            self.DRIVER_CTR_MOTOR_HV_SW: self._handle_dr_ctrl_motor_hv_sw,
            self.DRIVER_CTR_MPPT_HV_SW: self._handle_dr_ctrl_mppt_hv_sw,
            self.DRIVER_CTR_MOTOR_FR_SW: self._handle_dr_ctrl_motor_fr_sw,
            self.DRIVER_CTR_MOTOR_PE_SW: self._handle_dr_ctrl_motor_pe_sw,
            self.DRIVER_CTR_SERVICE_BR_SW: self._handle_dr_ctrl_service_br_sw,
            self.DRIVER_CTR_PARKING_BR_SW: self._handle_dr_ctrl_parking_br_sw,
            self.DRIVER_CTR_RIGHT_TURN_SW: self._handle_dr_ctrl_right_turn_sw,
            self.DRIVER_CTR_LEFT_TURN_SW: self._handle_dr_ctrl_left_turn_sw,
            self.REAR_CTRL_MOTOR_RPM: self._handle_rear_ctrl_motor_rpm,
            self.REAR_CTRL_VEHICLE_SPEED: self._handle_rear_ctrl_vehicle_speed,
            self.REAR_CTRL_ARR_VOL_1: self._handle_rear_ctrl_arr_vol_1,
            self.REAR_CTRL_ARR_CUR_1: self._handle_rear_ctrl_arr_cur_1,
            self.REAR_CTRL_BAT_MEAS_1: self._handle_rear_ctrl_bat_meas_1,
            self.REAR_CTRL_MPPT_TEMP_1: self._handle_rear_ctrl_mppt_temp_1,
            self.REAR_CTRL_ARR_VOL_2: self._handle_rear_ctrl_arr_vol_2,
            self.REAR_CTRL_ARR_CUR_2: self._handle_rear_ctrl_arr_cur_2,
            self.REAR_CTRL_BAT_MEAS_2: self._handle_rear_ctrl_bat_meas_2,
            self.REAR_CTRL_MPPT_TEMP_2: self._handle_rear_ctrl_mppt_temp_2,
            self.REAR_CTRL_ARR_VOL_3: self._handle_rear_ctrl_arr_vol_3,
            self.REAR_CTRL_ARR_CUR_3: self._handle_rear_ctrl_arr_cur_3,
            self.REAR_CTRL_BAT_MEAS_3: self._handle_rear_ctrl_bat_meas_3,
            self.REAR_CTRL_MPPT_TEMP_3: self._handle_rear_ctrl_mppt_temp_3,
            self.REAR_CTRL_ARR_VOL_4: self._handle_rear_ctrl_arr_vol_4,
            self.REAR_CTRL_ARR_CUR_4: self._handle_rear_ctrl_arr_cur_4,
            self.REAR_CTRL_BAT_MEAS_4: self._handle_rear_ctrl_bat_meas_4,
            self.REAR_CTRL_MPPT_TEMP_4: self._handle_rear_ctrl_mppt_temp_4,
            self.LV_BPS_BAT_VOL: self._handle_lv_bps_bat_vol,
            self.LV_BPS_BAT_CUR: self._handle_lv_bps_bat_cur,
            self.LV_BPS_SYS_CUR: self._handle_lv_bps_sys_cur,
            self.LV_BPS_BAT_TEMP: self._handle_lv_bps_bat_temp,
            self.HV_BPS_FAULT_FLAGS: self._handle_hv_bps_fault_flags,
            self.HV_BPS_MAIN_CONTACTOR_STATUS: self._handle_hv_bps_main_contactor_status,
            self.HV_BPS_MOTOR_CONTACTOR_STATUS: self._handle_hv_bps_motor_contactor_status,
            self.HV_BPS_MPPT_CONTACTOR_STATUS: self._handle_hv_bps_mppt_contactor_status,
            self.HV_BPS_BAT_VOL: self._handle_hv_bps_bat_vol,
            self.HV_BPS_BAT_CUR: self._handle_hv_bps_bat_cur,
            self.HV_BPS_BAT_AVG_TEMP: self._handle_hv_bps_bat_avg_temp,
            self.HV_BPS_BAT_MAX_TEMP: self._handle_hv_bps_bat_max_temp,
            self.HV_BPS_BAT_SOC: self._handle_hv_bps_bat_soc,
            self.HV_BPS_MOD_VOL_1: self._handle_hv_bps_mod_vol_1,
            self.HV_BPS_MOD_VOL_2: self._handle_hv_bps_mod_vol_2,
            self.HV_BPS_MOD_VOL_3: self._handle_hv_bps_mod_vol_3,
            self.HV_BPS_MOD_VOL_4: self._handle_hv_bps_mod_vol_4,
            self.HV_BPS_MOD_VOL_5: self._handle_hv_bps_mod_vol_5,
            self.HV_BPS_MOD_VOL_6: self._handle_hv_bps_mod_vol_6,
            self.HV_BPS_MOD_VOL_7: self._handle_hv_bps_mod_vol_7,
            self.HV_BPS_MOD_VOL_8: self._handle_hv_bps_mod_vol_8,
            self.HV_BPS_MOD_VOL_9: self._handle_hv_bps_mod_vol_9,
            self.HV_BPS_MOD_VOL_10: self._handle_hv_bps_mod_vol_10,
            self.HV_BPS_MOD_VOL_11: self._handle_hv_bps_mod_vol_11,
            self.HV_BPS_MOD_VOL_12: self._handle_hv_bps_mod_vol_12,
            self.HV_BPS_MOD_VOL_13: self._handle_hv_bps_mod_vol_13,
            self.HV_BPS_MOD_VOL_14: self._handle_hv_bps_mod_vol_14,
            self.HV_BPS_MOD_VOL_15: self._handle_hv_bps_mod_vol_15,
            self.HV_BPS_MOD_VOL_16: self._handle_hv_bps_mod_vol_16,
            self.HV_BPS_MOD_VOL_17: self._handle_hv_bps_mod_vol_17,
            self.HV_BPS_MOD_VOL_18: self._handle_hv_bps_mod_vol_18,
            self.HV_BPS_MOD_VOL_19: self._handle_hv_bps_mod_vol_19,
            self.HV_BPS_MOD_VOL_20: self._handle_hv_bps_mod_vol_20,
            self.HV_BPS_MOD_VOL_21: self._handle_hv_bps_mod_vol_21,
            self.HV_BPS_MOD_VOL_22: self._handle_hv_bps_mod_vol_22,
            self.HV_BPS_MOD_VOL_23: self._handle_hv_bps_mod_vol_23,
            self.HV_BPS_MOD_VOL_24: self._handle_hv_bps_mod_vol_24,
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
                    f" longtitude={msg.longtitude:.6f} "
                    f"latitude={msg.latitude:.6f} "
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

        point = (
            Point("temperature")
            .tag("message_id", f"0x{msg.message_id:08X}")
            .tag("message_type", "wifi" if msg.message_type == 0 else "cellular")
            .field("value", temperature_c)
        )

        print(f"[Telemetry] Temperature: {temperature_c:.2f} C")

        self._influx_writer.write_datapoint(point)

    def _handle_dr_ctrl_main_hv_sw(self, msg: DecodedMessage) -> None:
        switch_status = struct.unpack('<I', msg.payload[:4])[0]
        
        point = (
            Point("dr_ctrl_main_hv_sw")
            .tag("message_id", f"0x{msg.message_id:08X}")
            .tag("message_type", "wifi" if msg.message_type == 0 else "cellular")
            .field("value", switch_status)
        )
        
        print(f"[Telemetry] Driver Control Main HV Switch: {switch_status}")
        
        self._influx_writer.write_datapoint(point)

    def _handle_dr_ctrl_motor_hv_sw(self, msg: DecodedMessage) -> None:
        switch_status = struct.unpack('<I', msg.payload[:4])[0]
        
        point = (
            Point("dr_ctrl_motor_hv_sw")
            .tag("message_id", f"0x{msg.message_id:08X}")
            .tag("message_type", "wifi" if msg.message_type == 0 else "cellular")
            .field("value", switch_status)
        )
        
        print(f"[Telemetry] Driver Control Motor HV Switch: {switch_status}")
        
        self._influx_writer.write_datapoint(point)

    def _handle_dr_ctrl_mppt_hv_sw(self, msg: DecodedMessage) -> None:
        switch_status = struct.unpack('<I', msg.payload[:4])[0]
        
        point = (
            Point("dr_ctrl_mppt_hv_sw")
            .tag("message_id", f"0x{msg.message_id:08X}")
            .tag("message_type", "wifi" if msg.message_type == 0 else "cellular")
            .field("value", switch_status)
        )
        
        print(f"[Telemetry] Driver Control MPPT HV Switch: {switch_status}")
        
        self._influx_writer.write_datapoint(point)

    def _handle_dr_ctrl_motor_fr_sw(self, msg: DecodedMessage) -> None:
        switch_status = struct.unpack('<I', msg.payload[:4])[0]
        
        point = (
            Point("dr_ctrl_motor_fr_sw")
            .tag("message_id", f"0x{msg.message_id:08X}")
            .tag("message_type", "wifi" if msg.message_type == 0 else "cellular")
            .field("value", switch_status)
        )
        
        print(f"[Telemetry] Driver Control Motor Forward/Reverse Switch: {switch_status}")
        
        self._influx_writer.write_datapoint(point)

    def _handle_dr_ctrl_motor_pe_sw(self, msg: DecodedMessage) -> None:
        switch_status = struct.unpack('<I', msg.payload[:4])[0]

        point = (
            Point("dr_ctrl_motor_pe_sw")
            .tag("message_id", f"0x{msg.message_id:08X}")
            .tag("message_type", "wifi" if msg.message_type == 0 else "cellular")
            .field("value", switch_status)
        )

        print(f"[Telemetry] Driver Control Motor Power/Enable Switch: {switch_status}")

        self._influx_writer.write_datapoint(point)

    def _handle_dr_ctrl_service_br_sw(self, msg: DecodedMessage) -> None:
        switch_status = struct.unpack('<I', msg.payload[:4])[0]

        point = (
            Point("dr_ctrl_service_br_sw")
            .tag("message_id", f"0x{msg.message_id:08X}")
            .tag("message_type", "wifi" if msg.message_type == 0 else "cellular")
            .field("value", switch_status)
        )

        print(f"[Telemetry] Driver Control Service Brake Switch: {switch_status}")

        self._influx_writer.write_datapoint(point)

    def _handle_dr_ctrl_parking_br_sw(self, msg: DecodedMessage) -> None:
        switch_status = struct.unpack('<I', msg.payload[:4])[0]

        point = (
            Point("dr_ctrl_parking_br_sw")
            .tag("message_id", f"0x{msg.message_id:08X}")
            .tag("message_type", "wifi" if msg.message_type == 0 else "cellular")
            .field("value", switch_status)
        )

        print(f"[Telemetry] Driver Control Parking Brake Switch: {switch_status}")

        self._influx_writer.write_datapoint(point)

    def _handle_dr_ctrl_right_turn_sw(self, msg: DecodedMessage) -> None:
        switch_status = struct.unpack('<I', msg.payload[:4])[0]

        point = (
            Point("dr_ctrl_right_turn_sw")
            .tag("message_id", f"0x{msg.message_id:08X}")
            .tag("message_type", "wifi" if msg.message_type == 0 else "cellular")
            .field("value", switch_status)
        )

        print(f"[Telemetry] Driver Control Right Turn Switch: {switch_status}")

        self._influx_writer.write_datapoint(point)

    def _handle_dr_ctrl_left_turn_sw(self, msg: DecodedMessage) -> None:
        switch_status = struct.unpack('<I', msg.payload[:4])[0]

        point = (
            Point("dr_ctrl_left_turn_sw")
            .tag("message_id", f"0x{msg.message_id:08X}")
            .tag("message_type", "wifi" if msg.message_type == 0 else "cellular")
            .field("value", switch_status)
        )

        print(f"[Telemetry] Driver Control Left Turn Switch: {switch_status}")

        self._influx_writer.write_datapoint(point)

    def _handle_rear_ctrl_motor_rpm(self, msg: DecodedMessage) -> None:
        motor_rpm = struct.unpack('<I', msg.payload[:4])[0]
        
        point = (
            Point("rear_ctrl_motor_rpm")
            .tag("message_id", f"0x{msg.message_id:08X}")
            .tag("message_type", "wifi" if msg.message_type == 0 else "cellular")
            .field("value", motor_rpm)
        )
        
        print(f"[Telemetry] Rear Control Motor RPM: {motor_rpm} rpm")
        
        self._influx_writer.write_datapoint(point)

    def _handle_rear_ctrl_vehicle_speed(self, msg: DecodedMessage) -> None:
        vehicle_speed = struct.unpack('<I', msg.payload[:4])[0]
        
        point = (
            Point("rear_ctrl_vehicle_speed")
            .tag("message_id", f"0x{msg.message_id:08X}")
            .tag("message_type", "wifi" if msg.message_type == 0 else "cellular")
            .field("value", vehicle_speed)
        )
        
        print(f"[Telemetry] Rear Control Vehicle Speed: {vehicle_speed:.2f} km/h")
        
        self._influx_writer.write_datapoint(point)

    def _handle_rear_ctrl_arr_vol_1(self, msg: DecodedMessage) -> None:
        value = struct.unpack('<I', msg.payload[:4])[0]
        
        point = (
            Point("rear_ctrl_arr_vol_1")
            .tag("message_id", f"0x{msg.message_id:08X}")
            .tag("message_type", "wifi" if msg.message_type == 0 else "cellular")
            .field("value", value)
        )
        
        print(f"[Telemetry] Rear Control Array Voltage 1: {value} V")
        
        self._influx_writer.write_datapoint(point)

    def _handle_rear_ctrl_arr_cur_1(self, msg: DecodedMessage) -> None:
        value = struct.unpack('<I', msg.payload[:4])[0]
        
        point = (
            Point("rear_ctrl_arr_cur_1")
            .tag("message_id", f"0x{msg.message_id:08X}")
            .tag("message_type", "wifi" if msg.message_type == 0 else "cellular")
            .field("value", value)
        )
        
        print(f"[Telemetry] Rear Control Array Current 1: {value} A")
        
        self._influx_writer.write_datapoint(point)

    def _handle_rear_ctrl_bat_meas_1(self, msg: DecodedMessage) -> None:
        value = struct.unpack('<I', msg.payload[:4])[0]
        
        point = (
            Point("rear_ctrl_bat_meas_1")
            .tag("message_id", f"0x{msg.message_id:08X}")
            .tag("message_type", "wifi" if msg.message_type == 0 else "cellular")
            .field("value", value)
        )
        
        print(f"[Telemetry] Rear Control Battery Measurement 1: {value}")
        
        self._influx_writer.write_datapoint(point)

    def _handle_rear_ctrl_mppt_temp_1(self, msg: DecodedMessage) -> None:
        value = struct.unpack('<I', msg.payload[:4])[0]
        
        point = (
            Point("rear_ctrl_mppt_temp_1")
            .tag("message_id", f"0x{msg.message_id:08X}")
            .tag("message_type", "wifi" if msg.message_type == 0 else "cellular")
            .field("value", value)
        )
        
        print(f"[Telemetry] Rear Control MPPT Temperature 1: {value} C")
        
        self._influx_writer.write_datapoint(point)

    def _handle_rear_ctrl_arr_vol_2(self, msg: DecodedMessage) -> None:
        value = struct.unpack('<I', msg.payload[:4])[0]
        
        point = (
            Point("rear_ctrl_arr_vol_2")
            .tag("message_id", f"0x{msg.message_id:08X}")
            .tag("message_type", "wifi" if msg.message_type == 0 else "cellular")
            .field("value", value)
        )
        
        print(f"[Telemetry] Rear Control Array Voltage 2: {value} V")
        
        self._influx_writer.write_datapoint(point)

    def _handle_rear_ctrl_arr_cur_2(self, msg: DecodedMessage) -> None:
        value = struct.unpack('<I', msg.payload[:4])[0]
        
        point = (
            Point("rear_ctrl_arr_cur_2")
            .tag("message_id", f"0x{msg.message_id:08X}")
            .tag("message_type", "wifi" if msg.message_type == 0 else "cellular")
            .field("value", value)
        )
        
        print(f"[Telemetry] Rear Control Array Current 2: {value} A")
        
        self._influx_writer.write_datapoint(point)

    def _handle_rear_ctrl_bat_meas_2(self, msg: DecodedMessage) -> None:
        value = struct.unpack('<I', msg.payload[:4])[0]
        
        point = (
            Point("rear_ctrl_bat_meas_2")
            .tag("message_id", f"0x{msg.message_id:08X}")
            .tag("message_type", "wifi" if msg.message_type == 0 else "cellular")
            .field("value", value)
        )
        
        print(f"[Telemetry] Rear Control Battery Measurement 2: {value}")
        
        self._influx_writer.write_datapoint(point)

    def _handle_rear_ctrl_mppt_temp_2(self, msg: DecodedMessage) -> None:
        value = struct.unpack('<I', msg.payload[:4])[0]
        
        point = (
            Point("rear_ctrl_mppt_temp_2")
            .tag("message_id", f"0x{msg.message_id:08X}")
            .tag("message_type", "wifi" if msg.message_type == 0 else "cellular")
            .field("value", value)
        )
        
        print(f"[Telemetry] Rear Control MPPT Temperature 2: {value} C")
        
        self._influx_writer.write_datapoint(point)

    def _handle_rear_ctrl_arr_vol_3(self, msg: DecodedMessage) -> None:
        value = struct.unpack('<I', msg.payload[:4])[0]
        
        point = (
            Point("rear_ctrl_arr_vol_3")
            .tag("message_id", f"0x{msg.message_id:08X}")
            .tag("message_type", "wifi" if msg.message_type == 0 else "cellular")
            .field("value", value)
        )
        
        print(f"[Telemetry] Rear Control Array Voltage 3: {value} V")
        
        self._influx_writer.write_datapoint(point)

    def _handle_rear_ctrl_arr_cur_3(self, msg: DecodedMessage) -> None:
        value = struct.unpack('<I', msg.payload[:4])[0]
        
        point = (
            Point("rear_ctrl_arr_cur_3")
            .tag("message_id", f"0x{msg.message_id:08X}")
            .tag("message_type", "wifi" if msg.message_type == 0 else "cellular")
            .field("value", value)
        )
        
        print(f"[Telemetry] Rear Control Array Current 3: {value} A")
        
        self._influx_writer.write_datapoint(point)

    def _handle_rear_ctrl_bat_meas_3(self, msg: DecodedMessage) -> None:
        value = struct.unpack('<I', msg.payload[:4])[0]
        
        point = (
            Point("rear_ctrl_bat_meas_3")
            .tag("message_id", f"0x{msg.message_id:08X}")
            .tag("message_type", "wifi" if msg.message_type == 0 else "cellular")
            .field("value", value)
        )
        
        print(f"[Telemetry] Rear Control Battery Measurement 3: {value}")
        
        self._influx_writer.write_datapoint(point)

    def _handle_rear_ctrl_mppt_temp_3(self, msg: DecodedMessage) -> None:
        value = struct.unpack('<I', msg.payload[:4])[0]
        
        point = (
            Point("rear_ctrl_mppt_temp_3")
            .tag("message_id", f"0x{msg.message_id:08X}")
            .tag("message_type", "wifi" if msg.message_type == 0 else "cellular")
            .field("value", value)
        )
        
        print(f"[Telemetry] Rear Control MPPT Temperature 3: {value} C")
        
        self._influx_writer.write_datapoint(point)

    def _handle_rear_ctrl_arr_vol_4(self, msg: DecodedMessage) -> None:
        value = struct.unpack('<I', msg.payload[:4])[0]
        
        point = (
            Point("rear_ctrl_arr_vol_4")
            .tag("message_id", f"0x{msg.message_id:08X}")
            .tag("message_type", "wifi" if msg.message_type == 0 else "cellular")
            .field("value", value)
        )
        
        print(f"[Telemetry] Rear Control Array Voltage 4: {value} V")
        
        self._influx_writer.write_datapoint(point)

    def _handle_rear_ctrl_arr_cur_4(self, msg: DecodedMessage) -> None:
        value = struct.unpack('<I', msg.payload[:4])[0]
        
        point = (
            Point("rear_ctrl_arr_cur_4")
            .tag("message_id", f"0x{msg.message_id:08X}")
            .tag("message_type", "wifi" if msg.message_type == 0 else "cellular")
            .field("value", value)
        )
        
        print(f"[Telemetry] Rear Control Array Current 4: {value} A")
        
        self._influx_writer.write_datapoint(point)

    def _handle_rear_ctrl_bat_meas_4(self, msg: DecodedMessage) -> None:
        value = struct.unpack('<I', msg.payload[:4])[0]
        
        point = (
            Point("rear_ctrl_bat_meas_4")
            .tag("message_id", f"0x{msg.message_id:08X}")
            .tag("message_type", "wifi" if msg.message_type == 0 else "cellular")
            .field("value", value)
        )
        
        print(f"[Telemetry] Rear Control Battery Measurement 4: {value}")
        
        self._influx_writer.write_datapoint(point)

    def _handle_rear_ctrl_mppt_temp_4(self, msg: DecodedMessage) -> None:
        value = struct.unpack('<I', msg.payload[:4])[0]
        
        point = (
            Point("rear_ctrl_mppt_temp_4")
            .tag("message_id", f"0x{msg.message_id:08X}")
            .tag("message_type", "wifi" if msg.message_type == 0 else "cellular")
            .field("value", value)
        )
        
        print(f"[Telemetry] Rear Control MPPT Temperature 4: {value} C")
        
        self._influx_writer.write_datapoint(point)

    def _handle_lv_bps_bat_vol(self, msg: DecodedMessage) -> None:
        lv_bat_vol = struct.unpack('<I', msg.payload[:4])[0]

        # Multiple by 0.1 mV to convert to volts
        lv_bat_volts = lv_bat_vol * 0.0001

        print(f"[Telemetry] LV BPS Battery Voltage: {lv_bat_volts:.2f} V")

        msg_type = "wifi" if msg.message_type == 0 else "cellular"

        point = (
            Point("battery_lv_volt")
            .tag("message_id", f"0x{msg.message_id:08X}")
            .tag("message_type", msg_type)
            .field("value", lv_bat_volts)
        )

        self._influx_writer.write_datapoint(point)

    def _handle_lv_bps_bat_cur(self, msg: DecodedMessage) -> None:
        lv_bat_cur = struct.unpack('<I', msg.payload[:4])[0]

        # Multiple by 0.1 mA to convert to amps
        lv_bat_amps = lv_bat_cur * 0.0001

        print(f"[Telemetry] LV BPS Battery Current: {lv_bat_amps:.2f} A")

        msg_type = "wifi" if msg.message_type == 0 else "cellular"

        point = (
            Point("battery_lv_current")
            .tag("message_id", f"0x{msg.message_id:08X}")
            .tag("message_type", msg_type)
            .field("value", lv_bat_amps)
        )

        self._influx_writer.write_datapoint(point)

    def _handle_lv_bps_sys_cur(self, msg: DecodedMessage) -> None:
        lv_sys_cur = struct.unpack('<I', msg.payload[:4])[0]

        # Multiple by 0.1 mA to convert to amps
        lv_sys_amps = lv_sys_cur * 0.0001

        print(f"[Telemetry] LV BPS System Current: {lv_sys_amps:.2f} A")

        msg_type = "wifi" if msg.message_type == 0 else "cellular"

        point = (
            Point("battery_lv_system_current")
            .tag("message_id", f"0x{msg.message_id:08X}")
            .tag("message_type", msg_type)
            .field("value", lv_sys_amps)
        )

        self._influx_writer.write_datapoint(point)

    def _handle_lv_bps_bat_temp(self, msg: DecodedMessage) -> None:
        lv_bat_temp = struct.unpack('<I', msg.payload[:4])[0]

        print(f"[Telemetry] LV BPS Battery Temperature: {lv_bat_temp} C")

        msg_type = "wifi" if msg.message_type == 0 else "cellular"

        point = (
            Point("battery_lv_temp")
            .tag("message_id", f"0x{msg.message_id:08X}")
            .tag("message_type", msg_type)
            .field("value", lv_bat_temp)
        )

        self._influx_writer.write_datapoint(point)

    def _handle_hv_bps_fault_flags(self, msg: DecodedMessage) -> None:
        hv_fault = struct.unpack('<I', msg.payload[:4])[0]

        # Only 0 or 1
        print(f"[Telemetry] HV BPS Fault Flags: {hv_fault}")
        msg_type = "wifi" if msg.message_type == 0 else "cellular"
        point = (
            Point("battery_hv_fault")
            .tag("message_id", f"0x{msg.message_id:08X}")
            .tag("message_type", msg_type)
            .field("value", hv_fault)
        )

        self._influx_writer.write_datapoint(point)

    def _handle_hv_bps_main_contactor_status(self, msg: DecodedMessage) -> None:
        hv_main_contractor = struct.unpack('<I', msg.payload[:4])[0]

        print(f"[Telemetry] HV BPS Main Contactor Status: {hv_main_contractor}")
        msg_type = "wifi" if msg.message_type == 0 else "cellular"
        point = (
            Point("battery_hv_main_contractor")
            .tag("message_id", f"0x{msg.message_id:08X}")
            .tag("message_type", msg_type)
            .field("value", hv_main_contractor)
        )

        self._influx_writer.write_datapoint(point)

    def _handle_hv_bps_motor_contactor_status(self, msg: DecodedMessage) -> None:
        hv_motor_contractor = struct.unpack('<I', msg.payload[:4])[0]

        print(f"[Telemetry] HV BPS Motor Contactor Status: {hv_motor_contractor}")
        msg_type = "wifi" if msg.message_type == 0 else "cellular"
        point = (
            Point("battery_hv_motor_contractor")
            .tag("message_id", f"0x{msg.message_id:08X}")
            .tag("message_type", msg_type)
            .field("value", hv_motor_contractor)
        )

        self._influx_writer.write_datapoint(point)

    def _handle_hv_bps_mppt_contactor_status(self, msg: DecodedMessage) -> None:
        hv_mppt_contractor = struct.unpack('<I', msg.payload[:4])[0]

        print(f"[Telemetry] HV BPS MPPT Contactor Status: {hv_mppt_contractor}")
        msg_type = "wifi" if msg.message_type == 0 else "cellular"
        point = (
            Point("battery_hv_mppt_contractor")
            .tag("message_id", f"0x{msg.message_id:08X}")
            .tag("message_type", msg_type)
            .field("value", hv_mppt_contractor)
        )

        self._influx_writer.write_datapoint(point)

    def _handle_hv_bps_bat_vol(self, msg: DecodedMessage) -> None:
        bat_pack_voltage = struct.unpack('<I', msg.payload[:4])[0]

        # Multiple by 0.1 mV to convert to volts
        bat_pack_voltage_volts = bat_pack_voltage * 0.0001

        print(f"[Telemetry] HV BPS Battery Pack Voltage: {bat_pack_voltage_volts:.2f} V")
        msg_type = "wifi" if msg.message_type == 0 else "cellular"
        point = (
            Point("battery_pack_voltage")
            .tag("message_id", f"0x{msg.message_id:08X}")
            .tag("message_type", msg_type)
            .field("value", bat_pack_voltage_volts)
        )

        self._influx_writer.write_datapoint(point)

    def _handle_hv_bps_bat_cur(self, msg: DecodedMessage) -> None:
        bat_current = struct.unpack('<i', msg.payload[:4])[0]

        print(f"[Telemetry] HV BPS Battery Current: {bat_current:.2f} A")
        msg_type = "wifi" if msg.message_type == 0 else "cellular"
        point = (
            Point("battery_pack_current")
            .tag("message_id", f"0x{msg.message_id:08X}")
            .tag("message_type", msg_type)
            .field("value", bat_current)
        )

        self._influx_writer.write_datapoint(point)

    def _handle_hv_bps_bat_avg_temp(self, msg: DecodedMessage) -> None:
        bat_avg_temp = struct.unpack('<I', msg.payload[:4])[0]

        print(f"[Telemetry] HV BPS Battery Average Temperature: {bat_avg_temp} C")
        msg_type = "wifi" if msg.message_type == 0 else "cellular"
        point = (
            Point("battery_avg_temp")
            .tag("message_id", f"0x{msg.message_id:08X}")
            .tag("message_type", msg_type)
            .field("value", bat_avg_temp)
        )

        self._influx_writer.write_datapoint(point)

    def _handle_hv_bps_bat_max_temp(self, msg: DecodedMessage) -> None:
        bat_max_temp = struct.unpack('<I', msg.payload[:4])[0]

        print(f"[Telemetry] HV BPS Battery Max Temperature: {bat_max_temp} C")
        msg_type = "wifi" if msg.message_type == 0 else "cellular"
        point = (
            Point("battery_max_temp")
            .tag("message_id", f"0x{msg.message_id:08X}")
            .tag("message_type", msg_type)
            .field("value", bat_max_temp)
        )

        self._influx_writer.write_datapoint(point)

    def _handle_hv_bps_bat_soc(self, msg: DecodedMessage) -> None:
        # Assemble the first 4-bytes of the payload into a uint32
        bat_soc = struct.unpack('<I', msg.payload[:4])[0]

        # bat_soc is percentage in range of 0 to 100
        print(f"[Telemetry] HV BPS Battery State of Charge: {bat_soc} %")
        msg_type = "wifi" if msg.message_type == 0 else "cellular"
        point = (
            Point("battery_soc")
            .tag("message_id", f"0x{msg.message_id:08X}")
            .tag("message_type", msg_type)
            .field("value", bat_soc)
        )

        self._influx_writer.write_datapoint(point)

    def _handle_hv_bps_mod_vol_1(self, msg: DecodedMessage) -> None:
        bms_vol_1 = struct.unpack('<I', msg.payload[:4])[0]

        print(f"[Telemetry] HV BPS Module 1 Voltage: {bms_vol_1} mV")
        msg_type = "wifi" if msg.message_type == 0 else "cellular"
        point = (
            Point("battery_vol_module")
            .tag("message_id", f"0x{msg.message_id:08X}")
            .tag("message_type", msg_type)
            .field("module", 1)
            .field("value", bms_vol_1)
        )

        self._influx_writer.write_datapoint(point)

    def _handle_hv_bps_mod_vol_2(self, msg: DecodedMessage) -> None:
        bms_vol_2 = struct.unpack('<I', msg.payload[:4])[0]

        print(f"[Telemetry] HV BPS Module 2 Voltage: {bms_vol_2} mV")
        msg_type = "wifi" if msg.message_type == 0 else "cellular"
        point = (
            Point("battery_vol_module")
            .tag("message_id", f"0x{msg.message_id:08X}")
            .tag("message_type", msg_type)
            .field("module", 2)
            .field("value", bms_vol_2)
        )

        self._influx_writer.write_datapoint(point)

    def _handle_hv_bps_mod_vol_3(self, msg: DecodedMessage) -> None:
        bms_vol_3 = struct.unpack('<I', msg.payload[:4])[0]

        print(f"[Telemetry] HV BPS Module 3 Voltage: {bms_vol_3} mV")
        msg_type = "wifi" if msg.message_type == 0 else "cellular"
        point = (
            Point("battery_vol_module")
            .tag("message_id", f"0x{msg.message_id:08X}")
            .tag("message_type", msg_type)
            .field("module", 3)
            .field("value", bms_vol_3)
        )

        self._influx_writer.write_datapoint(point)

    def _handle_hv_bps_mod_vol_4(self, msg: DecodedMessage) -> None:
        bms_vol_4 = struct.unpack('<I', msg.payload[:4])[0]

        print(f"[Telemetry] HV BPS Module 4 Voltage: {bms_vol_4} mV")
        msg_type = "wifi" if msg.message_type == 0 else "cellular"
        point = (
            Point("battery_vol_module")
            .tag("message_id", f"0x{msg.message_id:08X}")
            .tag("message_type", msg_type)
            .field("module", 4)
            .field("value", bms_vol_4)
        )

        self._influx_writer.write_datapoint(point)

    def _handle_hv_bps_mod_vol_5(self, msg: DecodedMessage) -> None:
        bms_vol_5 = struct.unpack('<I', msg.payload[:4])[0]

        print(f"[Telemetry] HV BPS Module 5 Voltage: {bms_vol_5} mV")
        msg_type = "wifi" if msg.message_type == 0 else "cellular"
        point = (
            Point("battery_vol_module")
            .tag("message_id", f"0x{msg.message_id:08X}")
            .tag("message_type", msg_type)
            .field("module", 5)
            .field("value", bms_vol_5)
        )

        self._influx_writer.write_datapoint(point)

    def _handle_hv_bps_mod_vol_6(self, msg: DecodedMessage) -> None:
        bms_vol_6 = struct.unpack('<I', msg.payload[:4])[0]

        print(f"[Telemetry] HV BPS Module 6 Voltage: {bms_vol_6} mV")
        msg_type = "wifi" if msg.message_type == 0 else "cellular"
        point = (
            Point("battery_vol_module")
            .tag("message_id", f"0x{msg.message_id:08X}")
            .tag("message_type", msg_type)
            .field("module", 6)
            .field("value", bms_vol_6)
        )

        self._influx_writer.write_datapoint(point)

    def _handle_hv_bps_mod_vol_7(self, msg: DecodedMessage) -> None:
        bms_vol_7 = struct.unpack('<I', msg.payload[:4])[0]

        print(f"[Telemetry] HV BPS Module 7 Voltage: {bms_vol_7} mV")
        msg_type = "wifi" if msg.message_type == 0 else "cellular"
        point = (
            Point("battery_vol_module")
            .tag("message_id", f"0x{msg.message_id:08X}")
            .tag("message_type", msg_type)
            .field("module", 7)
            .field("value", bms_vol_7)
        )

        self._influx_writer.write_datapoint(point)

    def _handle_hv_bps_mod_vol_8(self, msg: DecodedMessage) -> None:
        bms_vol_8 = struct.unpack('<I', msg.payload[:4])[0]

        print(f"[Telemetry] HV BPS Module 8 Voltage: {bms_vol_8} mV")
        msg_type = "wifi" if msg.message_type == 0 else "cellular"
        point = (
            Point("battery_vol_module")
            .tag("message_id", f"0x{msg.message_id:08X}")
            .tag("message_type", msg_type)
            .field("module", 8)
            .field("value", bms_vol_8)
        )

        self._influx_writer.write_datapoint(point)

    def _handle_hv_bps_mod_vol_9(self, msg: DecodedMessage) -> None:
        bms_vol_9 = struct.unpack('<I', msg.payload[:4])[0]

        print(f"[Telemetry] HV BPS Module 9 Voltage: {bms_vol_9} mV")
        msg_type = "wifi" if msg.message_type == 0 else "cellular"
        point = (
            Point("battery_vol_module")
            .tag("message_id", f"0x{msg.message_id:08X}")
            .tag("message_type", msg_type)
            .field("module", 9)
            .field("value", bms_vol_9)
        )

        self._influx_writer.write_datapoint(point)

    def _handle_hv_bps_mod_vol_10(self, msg: DecodedMessage) -> None:
        bms_vol_10 = struct.unpack('<I', msg.payload[:4])[0]

        print(f"[Telemetry] HV BPS Module 10 Voltage: {bms_vol_10} mV")
        msg_type = "wifi" if msg.message_type == 0 else "cellular"
        point = (
            Point("battery_vol_module")
            .tag("message_id", f"0x{msg.message_id:08X}")
            .tag("message_type", msg_type)
            .field("module", 10)
            .field("value", bms_vol_10)
        )

        self._influx_writer.write_datapoint(point)

    def _handle_hv_bps_mod_vol_11(self, msg: DecodedMessage) -> None:
        bms_vol_11 = struct.unpack('<I', msg.payload[:4])[0]

        print(f"[Telemetry] HV BPS Module 11 Voltage: {bms_vol_11} mV")
        msg_type = "wifi" if msg.message_type == 0 else "cellular"
        point = (
            Point("battery_vol_module")
            .tag("message_id", f"0x{msg.message_id:08X}")
            .tag("message_type", msg_type)
            .field("module", 11)
            .field("value", bms_vol_11)
        )

        self._influx_writer.write_datapoint(point)

    def _handle_hv_bps_mod_vol_12(self, msg: DecodedMessage) -> None:
        bms_vol_12 = struct.unpack('<I', msg.payload[:4])[0]

        print(f"[Telemetry] HV BPS Module 12 Voltage: {bms_vol_12} mV")
        msg_type = "wifi" if msg.message_type == 0 else "cellular"
        point = (
            Point("battery_vol_module")
            .tag("message_id", f"0x{msg.message_id:08X}")
            .tag("message_type", msg_type)
            .field("module", 12)
            .field("value", bms_vol_12)
        )

        self._influx_writer.write_datapoint(point)

    def _handle_hv_bps_mod_vol_13(self, msg: DecodedMessage) -> None:
        bms_vol_13 = struct.unpack('<I', msg.payload[:4])[0]

        print(f"[Telemetry] HV BPS Module 13 Voltage: {bms_vol_13} mV")
        msg_type = "wifi" if msg.message_type == 0 else "cellular"
        point = (
            Point("battery_vol_module")
            .tag("message_id", f"0x{msg.message_id:08X}")
            .tag("message_type", msg_type)
            .field("module", 13)
            .field("value", bms_vol_13)
        )

        self._influx_writer.write_datapoint(point)

    def _handle_hv_bps_mod_vol_14(self, msg: DecodedMessage) -> None:
        bms_vol_14 = struct.unpack('<I', msg.payload[:4])[0]

        print(f"[Telemetry] HV BPS Module 14 Voltage: {bms_vol_14} mV")
        msg_type = "wifi" if msg.message_type == 0 else "cellular"
        point = (
            Point("battery_vol_module")
            .tag("message_id", f"0x{msg.message_id:08X}")
            .tag("message_type", msg_type)
            .field("module", 14)
            .field("value", bms_vol_14)
        )

        self._influx_writer.write_datapoint(point)

    def _handle_hv_bps_mod_vol_15(self, msg: DecodedMessage) -> None:
        bms_vol_15 = struct.unpack('<I', msg.payload[:4])[0]

        print(f"[Telemetry] HV BPS Module 15 Voltage: {bms_vol_15} mV")
        msg_type = "wifi" if msg.message_type == 0 else "cellular"
        point = (
            Point("battery_vol_module")
            .tag("message_id", f"0x{msg.message_id:08X}")
            .tag("message_type", msg_type)
            .field("module", 15)
            .field("value", bms_vol_15)
        )

        self._influx_writer.write_datapoint(point)

    def _handle_hv_bps_mod_vol_16(self, msg: DecodedMessage) -> None:
        bms_vol_16 = struct.unpack('<I', msg.payload[:4])[0]

        print(f"[Telemetry] HV BPS Module 16 Voltage: {bms_vol_16} mV")
        msg_type = "wifi" if msg.message_type == 0 else "cellular"
        point = (
            Point("battery_vol_module")
            .tag("message_id", f"0x{msg.message_id:08X}")
            .tag("message_type", msg_type)
            .field("module", 16)
            .field("value", bms_vol_16)
        )

        self._influx_writer.write_datapoint(point)

    def _handle_hv_bps_mod_vol_17(self, msg: DecodedMessage) -> None:
        bms_vol_17 = struct.unpack('<I', msg.payload[:4])[0]

        print(f"[Telemetry] HV BPS Module 17 Voltage: {bms_vol_17} mV")
        msg_type = "wifi" if msg.message_type == 0 else "cellular"
        point = (
            Point("battery_vol_module")
            .tag("message_id", f"0x{msg.message_id:08X}")
            .tag("message_type", msg_type)
            .field("module", 17)
            .field("value", bms_vol_17)
        )

        self._influx_writer.write_datapoint(point)

    def _handle_hv_bps_mod_vol_18(self, msg: DecodedMessage) -> None:
        bms_vol_18 = struct.unpack('<I', msg.payload[:4])[0]

        print(f"[Telemetry] HV BPS Module 18 Voltage: {bms_vol_18} mV")
        msg_type = "wifi" if msg.message_type == 0 else "cellular"
        point = (
            Point("battery_vol_module")
            .tag("message_id", f"0x{msg.message_id:08X}")
            .tag("message_type", msg_type)
            .field("module", 18)
            .field("value", bms_vol_18)
        )

        self._influx_writer.write_datapoint(point)

    def _handle_hv_bps_mod_vol_19(self, msg: DecodedMessage) -> None:
        bms_vol_19 = struct.unpack('<I', msg.payload[:4])[0]

        print(f"[Telemetry] HV BPS Module 19 Voltage: {bms_vol_19} mV")
        msg_type = "wifi" if msg.message_type == 0 else "cellular"
        point = (
            Point("battery_vol_module")
            .tag("message_id", f"0x{msg.message_id:08X}")
            .tag("message_type", msg_type)
            .field("module", 19)
            .field("value", bms_vol_19)
        )

        self._influx_writer.write_datapoint(point)

    def _handle_hv_bps_mod_vol_20(self, msg: DecodedMessage) -> None:
        bms_vol_20 = struct.unpack('<I', msg.payload[:4])[0]

        print(f"[Telemetry] HV BPS Module 20 Voltage: {bms_vol_20} mV")
        msg_type = "wifi" if msg.message_type == 0 else "cellular"
        point = (
            Point("battery_vol_module")
            .tag("message_id", f"0x{msg.message_id:08X}")
            .tag("message_type", msg_type)
            .field("module", 20)
            .field("value", bms_vol_20)
        )

        self._influx_writer.write_datapoint(point)

    def _handle_hv_bps_mod_vol_21(self, msg: DecodedMessage) -> None:
        bms_vol_21 = struct.unpack('<I', msg.payload[:4])[0]

        print(f"[Telemetry] HV BPS Module 21 Voltage: {bms_vol_21} mV")
        msg_type = "wifi" if msg.message_type == 0 else "cellular"
        point = (
            Point("battery_vol_module")
            .tag("message_id", f"0x{msg.message_id:08X}")
            .tag("message_type", msg_type)
            .field("module", 21)
            .field("value", bms_vol_21)
        )

        self._influx_writer.write_datapoint(point)

    def _handle_hv_bps_mod_vol_22(self, msg: DecodedMessage) -> None:
        bms_vol_22 = struct.unpack('<I', msg.payload[:4])[0]

        print(f"[Telemetry] HV BPS Module 22 Voltage: {bms_vol_22} mV")
        msg_type = "wifi" if msg.message_type == 0 else "cellular"
        point = (
            Point("battery_vol_module")
            .tag("message_id", f"0x{msg.message_id:08X}")
            .tag("message_type", msg_type)
            .field("module", 22)
            .field("value", bms_vol_22)
        )

        self._influx_writer.write_datapoint(point)

    def _handle_hv_bps_mod_vol_23(self, msg: DecodedMessage) -> None:
        bms_vol_23 = struct.unpack('<I', msg.payload[:4])[0]

        print(f"[Telemetry] HV BPS Module 23 Voltage: {bms_vol_23} mV")
        msg_type = "wifi" if msg.message_type == 0 else "cellular"
        point = (
            Point("battery_vol_module")
            .tag("message_id", f"0x{msg.message_id:08X}")
            .tag("message_type", msg_type)
            .field("module", 23)
            .field("value", bms_vol_23)
        )

        self._influx_writer.write_datapoint(point)

    def _handle_hv_bps_mod_vol_24(self, msg: DecodedMessage) -> None:
        bms_vol_24 = struct.unpack('<I', msg.payload[:4])[0]

        print(f"[Telemetry] HV BPS Module 24 Voltage: {bms_vol_24} mV")
        msg_type = "wifi" if msg.message_type == 0 else "cellular"
        point = (
            Point("battery_vol_module")
            .tag("message_id", f"0x{msg.message_id:08X}")
            .tag("message_type", msg_type)
            .field("module", 24)
            .field("value", bms_vol_24)
        )

        self._influx_writer.write_datapoint(point)

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