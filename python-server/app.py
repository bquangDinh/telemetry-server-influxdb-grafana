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
            .tag("message_type", f"{"wifi" if msg.message_type == 0 else "cellular"}")
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
        
    def _handle_dr_ctrl_main_hv_sw(self, msg: DecodedMessage) -> None:
        pass  # TODO: implement handling for driver control main HV switch
    
    def _handle_dr_ctrl_motor_hv_sw(self, msg: DecodedMessage) -> None:
        pass  # TODO: implement handling for driver control motor HV switch
    
    def _handle_dr_ctrl_mppt_hv_sw(self, msg: DecodedMessage) -> None:
        pass  # TODO: implement handling for driver control MPPT HV switch
    
    def _handle_dr_ctrl_motor_fr_sw(self, msg: DecodedMessage) -> None:
        pass  # TODO: implement handling for driver control motor forward/reverse switch    
    
    def _handle_dr_ctrl_motor_pe_sw(self, msg: DecodedMessage) -> None:
        pass  # TODO: implement handling for driver control motor power/enable switch
    
    def _handle_dr_ctrl_service_br_sw(self, msg: DecodedMessage) -> None:
        pass  # TODO: implement handling for driver control service brake switch
    
    def _handle_dr_ctrl_parking_br_sw(self, msg: DecodedMessage) -> None:
        pass  # TODO: implement handling for driver control parking brake switch
    
    def _handle_dr_ctrl_right_turn_sw(self, msg: DecodedMessage) -> None:
        pass  # TODO: implement handling for driver control right turn switch
    
    def _handle_dr_ctrl_left_turn_sw(self, msg: DecodedMessage) -> None:
        pass  # TODO: implement handling for driver control left turn switch
    
    def _handle_rear_ctrl_motor_rpm(self, msg: DecodedMessage) -> None:
        pass  # TODO: implement handling for rear control motor RPM
    
    def _handle_rear_ctrl_vehicle_speed(self, msg: DecodedMessage) -> None:
        pass  # TODO: implement handling for rear control vehicle speed
    
    def _handle_rear_ctrl_arr_vol_1(self, msg: DecodedMessage) -> None:
        pass  # TODO: implement handling for rear control array voltage 1
    
    def _handle_rear_ctrl_arr_cur_1(self, msg: DecodedMessage) -> None:
        pass  # TODO: implement handling for rear control array current 1
    
    def _handle_rear_ctrl_bat_meas_1(self, msg: DecodedMessage) -> None:
        pass  # TODO: implement handling for rear control battery measurement 1
    
    def _handle_rear_ctrl_mppt_temp_1(self, msg: DecodedMessage) -> None:
        pass  # TODO: implement handling for rear control MPPT temperature 1
    
    def _handle_rear_ctrl_arr_vol_2(self, msg: DecodedMessage) -> None:
        pass  # TODO: implement handling for rear control array voltage 2
    
    def _handle_rear_ctrl_arr_cur_2(self, msg: DecodedMessage) -> None:
        pass  # TODO: implement handling for rear control array current 2
    
    def _handle_rear_ctrl_bat_meas_2(self, msg: DecodedMessage) -> None:
        pass  # TODO: implement handling for rear control battery measurement 2
    
    def _handle_rear_ctrl_mppt_temp_2(self, msg: DecodedMessage) -> None:
        pass  # TODO: implement handling for rear control MPPT temperature 2
    
    def _handle_rear_ctrl_arr_vol_3(self, msg: DecodedMessage) -> None:
        pass  # TODO: implement handling for rear control array voltage 3
    
    def _handle_rear_ctrl_arr_cur_3(self, msg: DecodedMessage) -> None:
        pass  # TODO: implement handling for rear control array current 3
    
    def _handle_rear_ctrl_bat_meas_3(self, msg: DecodedMessage) -> None:
        pass  # TODO: implement handling for rear control battery measurement 3
    
    def _handle_rear_ctrl_mppt_temp_3(self, msg: DecodedMessage) -> None:
        pass  # TODO: implement handling for rear control MPPT temperature 3
    
    def _handle_rear_ctrl_arr_vol_4(self, msg: DecodedMessage) -> None:
        pass  # TODO: implement handling for rear control array voltage 4
    
    def _handle_rear_ctrl_arr_cur_4(self, msg: DecodedMessage) -> None:
        pass  # TODO: implement handling for rear control array current 4
    
    def _handle_rear_ctrl_bat_meas_4(self, msg: DecodedMessage) -> None:
        pass  # TODO: implement handling for rear control battery measurement 4
    
    def _handle_rear_ctrl_mppt_temp_4(self, msg: DecodedMessage) -> None:
        pass  # TODO: implement handling for rear control MPPT temperature 4
    
    def _handle_lv_bps_bat_vol(self, msg: DecodedMessage) -> None:
        pass  # TODO: implement handling for LV BPS battery voltage
    
    def _handle_lv_bps_bat_cur(self, msg: DecodedMessage) -> None:
        pass  # TODO: implement handling for LV BPS battery current
    
    def _handle_lv_bps_sys_cur(self, msg: DecodedMessage) -> None:
        pass  # TODO: implement handling for LV BPS system current
    
    def _handle_lv_bps_bat_temp(self, msg: DecodedMessage) -> None:
        pass  # TODO: implement handling for LV BPS battery temperature
    
    def _handle_hv_bps_fault_flags(self, msg: DecodedMessage) -> None:
        pass  # TODO: implement handling for HV BPS fault flags
    
    def _handle_hv_bps_main_contactor_status(self, msg: DecodedMessage) -> None:
        pass  # TODO: implement handling for HV BPS main contactor status
    
    def _handle_hv_bps_motor_contactor_status(self, msg: DecodedMessage) -> None:
        pass  # TODO: implement handling for HV BPS motor contactor status
    
    def _handle_hv_bps_mppt_contactor_status(self, msg: DecodedMessage) -> None:
        pass  # TODO: implement handling for HV BPS MPPT contactor status
    
    def _handle_hv_bps_bat_vol(self, msg: DecodedMessage) -> None:
        pass  # TODO: implement handling for HV BPS battery voltage
    
    def _handle_hv_bps_bat_cur(self, msg: DecodedMessage) -> None:
        pass  # TODO: implement handling for HV BPS battery current
    
    def _handle_hv_bps_bat_avg_temp(self, msg: DecodedMessage) -> None:
        pass  # TODO: implement handling for HV BPS battery average temperature
    
    def _handle_hv_bps_bat_max_temp(self, msg: DecodedMessage) -> None:
        pass  # TODO: implement handling for HV BPS battery maximum temperature
    
    def _handle_hv_bps_bat_soc(self, msg: DecodedMessage) -> None:
        pass  # TODO: implement handling for HV BPS battery state of charge
    
    def _handle_hv_bps_mod_vol_1(self, msg: DecodedMessage) -> None:
        pass  # TODO: implement handling for HV BPS module voltage 1

    def _handle_hv_bps_mod_vol_2(self, msg: DecodedMessage) -> None:
        pass  # TODO: implement handling for HV BPS module voltage 2
    
    def _handle_hv_bps_mod_vol_3(self, msg: DecodedMessage) -> None:
        pass  # TODO: implement handling for HV BPS module voltage 3
    
    def _handle_hv_bps_mod_vol_4(self, msg: DecodedMessage) -> None:
        pass  # TODO: implement handling for HV BPS module voltage 4
    
    def _handle_hv_bps_mod_vol_5(self, msg: DecodedMessage) -> None:
        pass  # TODO: implement handling for HV BPS module voltage 5
    
    def _handle_hv_bps_mod_vol_6(self, msg: DecodedMessage) -> None:
        pass  # TODO: implement handling for HV BPS module voltage 6
    
    def _handle_hv_bps_mod_vol_7(self, msg: DecodedMessage) -> None:
        pass  # TODO: implement handling for HV BPS module voltage 7
    
    def _handle_hv_bps_mod_vol_8(self, msg: DecodedMessage) -> None:
        pass  # TODO: implement handling for HV BPS module voltage 8
    
    def _handle_hv_bps_mod_vol_9(self, msg: DecodedMessage) -> None:
        pass  # TODO: implement handling for HV BPS module voltage 9
    
    def _handle_hv_bps_mod_vol_10(self, msg: DecodedMessage) -> None:
        pass  # TODO: implement handling for HV BPS module voltage 10
    
    def _handle_hv_bps_mod_vol_11(self, msg: DecodedMessage) -> None:
        pass  # TODO: implement handling for HV BPS module voltage 11
    
    def _handle_hv_bps_mod_vol_12(self, msg: DecodedMessage) -> None:
        pass  # TODO: implement handling for HV BPS module voltage 12
    
    def _handle_hv_bps_mod_vol_13(self, msg: DecodedMessage) -> None:
        pass  # TODO: implement handling for HV BPS module voltage 13
    
    def _handle_hv_bps_mod_vol_14(self, msg: DecodedMessage) -> None:
        pass  # TODO: implement handling for HV BPS module voltage 14
    
    def _handle_hv_bps_mod_vol_15(self, msg: DecodedMessage) -> None:
        pass  # TODO: implement handling for HV BPS module voltage 15
    
    def _handle_hv_bps_mod_vol_16(self, msg: DecodedMessage) -> None:
        pass  # TODO: implement handling for HV BPS module voltage 16
    
    def _handle_hv_bps_mod_vol_17(self, msg: DecodedMessage) -> None:
        pass  # TODO: implement handling for HV BPS module voltage 17
    
    def _handle_hv_bps_mod_vol_18(self, msg: DecodedMessage) -> None:
        pass  # TODO: implement handling for HV BPS module voltage 18
    
    def _handle_hv_bps_mod_vol_19(self, msg: DecodedMessage) -> None:
        pass  # TODO: implement handling for HV BPS module voltage 19
    
    def _handle_hv_bps_mod_vol_20(self, msg: DecodedMessage) -> None:
        pass  # TODO: implement handling for HV BPS module voltage 20
    
    def _handle_hv_bps_mod_vol_21(self, msg: DecodedMessage) -> None:
        pass  # TODO: implement handling for HV BPS module voltage 21
    
    def _handle_hv_bps_mod_vol_22(self, msg: DecodedMessage) -> None:
        pass  # TODO: implement handling for HV BPS module voltage 22
    
    def _handle_hv_bps_mod_vol_23(self, msg: DecodedMessage) -> None:
        pass  # TODO: implement handling for HV BPS module voltage 23
    
    def _handle_hv_bps_mod_vol_24(self, msg: DecodedMessage) -> None:
        pass  # TODO: implement handling for HV BPS module voltage 24

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