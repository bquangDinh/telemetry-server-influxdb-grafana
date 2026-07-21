print('Python Notecard Proxy starting...', flush=True)

from dataclasses import dataclass
import struct
import requests

from flask import Flask, request
import socket
import json
import os

ADDON_TELEMETRY_URL = os.getenv("ADDON_TELEMETRY_URL") or "https://purveyor-tattered-abroad.ngrok-free.dev/telemetry"
ADDON_TELEMETRY_BEARER_TOKEN = os.getenv("ADDON_TELEMETRY_BEARER_TOKEN")

# =========================
# CONFIG
# =========================
UDP_IP = os.getenv("UDP_IP")
UDP_PORT = int(os.getenv("UDP_PORT"))

HTTP_HOST = "0.0.0.0"
HTTP_PORT = int(os.getenv("TCP_PROXY_PORT", 8082))

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
# INIT
# =========================
app = Flask(__name__)

udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

# =========================
# ROUTE
# =========================
@app.route("/notecard", methods=["POST"])
def notecard():

    try:
        # Parse JSON from Notehub
        data = request.get_json()

        print("Received HTTP POST:")

        print(json.dumps(data, indent=2))

        if data is None:
            return "Invalid JSON", 400

        # Convert json to data model before converting to bytes

        if "file" not in data:
            return "Missing 'file' field", 400

        if "body" not in data:
            return "Missing 'body' field", 400

        # Transform data
        body = data.get("body", {})

        payload_hex = body.get("payload", "")
        payload = bytes.fromhex(payload_hex)

        payload_length = body.get("len", len(payload))

        if payload_length != len(payload):
            return f"Payload length mismatch: len={payload_length}, actual={len(payload)}", 400

        longitude = float(data.get("best_lon", 0.0))
        latitude = float(data.get("best_lat", 0.0))

        decoded_message = DecodedMessage(
            message_type=1,
            message_id=int(body.get("id", 0)),
            length=payload_length,
            payload=payload,
            longtitude=longitude,
            latitude=latitude
        )

        message_bytes = bytearray()

        message_bytes.append(decoded_message.message_type)
        message_bytes.extend(struct.pack("<I", decoded_message.message_id))
        message_bytes.append(decoded_message.length)
        message_bytes.extend(decoded_message.payload)
        message_bytes.extend(struct.pack("<ff", decoded_message.longtitude, decoded_message.latitude))

        udp_socket.sendto(message_bytes, (UDP_IP, UDP_PORT))

        print("Forwarded UDP packet:")
        print(json.dumps(data, indent=2))

        formatted_data = {
            "id": decoded_message.message_id,
            "len": decoded_message.length,
            "payload": decoded_message.payload.hex(),
            "longitude": decoded_message.longtitude,
            "latitude": decoded_message.latitude
        }

        try:
            route_to_addon_telemetry(formatted_data)
        except Exception as e:
            print(f"Error routing telemetry to addon: {e}")

        return "", 204

    except Exception as e:
        print("ERROR:", e)
        return "Internal Server Error", 500

@app.route("/health", methods=["GET"])
def health():
    return "OK", 200

def route_to_addon_telemetry(data):
	try:
		headers = {
      		"Authorization": ADDON_TELEMETRY_BEARER_TOKEN,
			"Content-Type": "application/json"
      	}

		response = requests.post(ADDON_TELEMETRY_URL, json=data, headers=headers)

		if response.status_code != 200:
			print(f"Failed to forward telemetry to addon: {response.status_code} - {response.text}")
	except Exception as e:
		print(f"Error forwarding telemetry to addon: {e}")
		print(f"Error forwarding telemetry to addon: {e}")

# =========================
# MAIN
# =========================
if __name__ == "__main__":
    print(f"HTTP server listening on {HTTP_HOST}:{HTTP_PORT}")
    print(f"Forwarding UDP to {UDP_IP}:{UDP_PORT}")

    app.run(
        host=HTTP_HOST,
        port=HTTP_PORT
    )