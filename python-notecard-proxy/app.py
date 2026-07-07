print('Python Notecard Proxy starting...', flush=True)

from dataclasses import dataclass
import struct

from flask import Flask, request
import socket
import json
import os

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
        # Check if payload is present and encode it to bytes
        if "payload" not in data:
            return "Missing 'payload' in request", 400
        
        payload = data.get("payload", "").encode("utf-8")
        
        len = len(payload)
        
        decoded_message = DecodedMessage(
            message_type=1,
            message_id=data.get("id", 0),
            length=len,
            payload=payload
        )
        
        # Convert decoded message to bytes
        message_bytes = bytearray()
        
        message_bytes.append(decoded_message.message_type)        
        message_bytes.extend(struct.pack("<I", decoded_message.message_id))
        message_bytes.append(decoded_message.length)
        message_bytes.extend(decoded_message.payload)

        # Forward over UDP
        udp_socket.sendto(message_bytes, (UDP_IP, UDP_PORT))

        print("Forwarded UDP packet:")
        print(json.dumps(data, indent=2))

        return "", 204

    except Exception as e:
        print("ERROR:", e)
        return "Internal Server Error", 500

@app.route("/health", methods=["GET"])
def health():
    return "OK", 200

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