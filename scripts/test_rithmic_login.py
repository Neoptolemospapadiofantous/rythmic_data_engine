"""
Minimal Rithmic login tester — no generated pb2 files required.
Encodes RequestLogin manually using raw protobuf wire format,
then decodes ResponseLogin to show rp_code and any messages.

Usage:
    python3 scripts/test_rithmic_login.py \
        --user <order_user> \
        --password <order_password> \
        --system LegendsTrading \
        --plant ORDER_PLANT

    # MD plant test (AMP credentials)
    python3 scripts/test_rithmic_login.py \
        --user <amp_user> \
        --password <amp_password> \
        --system "Rithmic 01" \
        --plant TICKER_PLANT
"""

import argparse
import asyncio
import ssl
import struct
import sys

try:
    import websockets
except ImportError:
    print("pip install websockets")
    sys.exit(1)

RITHMIC_URL = "wss://ritpz01001.01.rithmic.com:443"

# SysInfraType enum values
INFRA = {"TICKER_PLANT": 1, "ORDER_PLANT": 2, "HISTORY_PLANT": 3}

# ---- minimal protobuf encoder -----------------------------------------------

def _varint(n):
    out = []
    while n > 0x7F:
        out.append((n & 0x7F) | 0x80)
        n >>= 7
    out.append(n)
    return bytes(out)


def _field_varint(field_num, value):
    tag = (field_num << 3) | 0   # wire type 0 = varint
    return _varint(tag) + _varint(value)


def _field_string(field_num, value: str):
    tag = (field_num << 3) | 2   # wire type 2 = length-delimited
    encoded = value.encode()
    return _varint(tag) + _varint(len(encoded)) + encoded


def build_request_login(user, password, system_name, infra_type, app_name="rithmic_login_test", app_version="1.0"):
    # Field numbers from rithmic.proto:
    #   template_id=154467  template_version=153634  user=131003
    #   password=130004     app_name=130002           app_version=131803
    #   system_name=153628  infra_type=153621
    msg = b""
    msg += _field_varint(154467, 10)                    # template_id = 10 (RequestLogin)
    msg += _field_string(153634, "3.9")                 # template_version
    msg += _field_string(131003, user)
    msg += _field_string(130004, password)
    msg += _field_string(130002, app_name)
    msg += _field_string(131803, app_version)
    msg += _field_string(153628, system_name)
    msg += _field_varint(153621, infra_type)
    return msg


# ---- minimal protobuf decoder -----------------------------------------------

def _read_varint(data, pos):
    result = 0
    shift = 0
    while pos < len(data):
        b = data[pos]; pos += 1
        result |= (b & 0x7F) << shift
        shift += 7
        if not (b & 0x80):
            break
    return result, pos


def decode_response(data: bytes) -> dict:
    pos = 0
    fields = {}
    while pos < len(data):
        tag, pos = _read_varint(data, pos)
        field_num = tag >> 3
        wire_type = tag & 0x7
        if wire_type == 0:
            val, pos = _read_varint(data, pos)
            fields.setdefault(field_num, []).append(val)
        elif wire_type == 2:
            length, pos = _read_varint(data, pos)
            val = data[pos:pos+length]
            pos += length
            try:
                val = val.decode()
            except Exception:
                pass
            fields.setdefault(field_num, []).append(val)
        else:
            print(f"  [warn] unknown wire type {wire_type} at field {field_num}, stopping parse")
            break
    return fields


# ---- framing (4-byte big-endian length prefix) ------------------------------

def frame(payload: bytes) -> bytes:
    return struct.pack(">I", len(payload)) + payload


async def _recv_frame(ws) -> bytes:
    raw = await ws.recv()
    if isinstance(raw, str):
        raw = raw.encode()
    if len(raw) < 4:
        return raw
    length = struct.unpack(">I", raw[:4])[0]
    body = raw[4:]
    # websockets may deliver the rest in subsequent messages
    while len(body) < length:
        chunk = await ws.recv()
        body += chunk if isinstance(chunk, bytes) else chunk.encode()
    return body[:length]


# ---- main test --------------------------------------------------------------

CERT_PATH = "/home/theone/Desktop/rithmic_engine/certs/rithmic_ssl_cert_auth_params"


def build_request_system_info():
    # RequestRithmicSystemInfo: template_id field=154467, value=16
    return _field_varint(154467, 16)


def _make_ssl_ctx():
    ssl_ctx = ssl.create_default_context()
    try:
        ssl_ctx.load_verify_locations(CERT_PATH)
    except Exception:
        ssl_ctx.check_hostname = False
        ssl_ctx.verify_mode = ssl.CERT_NONE
    ssl_ctx.check_hostname = False
    return ssl_ctx


async def test_login(user, password, system_name, plant, url):
    infra_type = INFRA.get(plant, 2)
    ssl_ctx = _make_ssl_ctx()

    print(f"\n=== Step 1: probe system info ===")
    print(f"Connecting to {url} ...")
    try:
        async with websockets.connect(url, ssl=ssl_ctx, max_size=2**20) as ws:
            print("WebSocket connected — sending RequestRithmicSystemInfo (template_id=16)")
            await ws.send(frame(build_request_system_info()))

            # Read until we get template_id=17 (ResponseRithmicSystemInfo)
            systems = []
            for _ in range(5):
                raw = await asyncio.wait_for(ws.recv(), timeout=10)
                if isinstance(raw, str):
                    raw = raw.encode()
                # Strip 4-byte length prefix if present
                body = raw[4:] if len(raw) > 4 and struct.unpack(">I", raw[:4])[0] == len(raw) - 4 else raw
                decoded = decode_response(body)
                tid = decoded.get(154467, [None])[0]
                if tid == 17:
                    systems = decoded.get(153628, [])
                    break

            print(f"Available systems: {systems}")
            if system_name not in systems:
                print(f"WARNING: '{system_name}' NOT in list")
            else:
                print(f"System '{system_name}' confirmed OK")

    except asyncio.TimeoutError:
        print("ERROR: Timed out on system info")
        return
    except Exception as e:
        print(f"ERROR: {e}")
        return

    print(f"\n=== Step 2: login ===")
    try:
        async with websockets.connect(url, ssl=ssl_ctx, max_size=2**20) as ws:
            print(f"Reconnected — sending RequestLogin")
            print(f"  user={user}  system={system_name}  plant={plant}")
            payload = build_request_login(user, password, system_name, infra_type)
            await ws.send(frame(payload))

            # Read until template_id=11 (ResponseLogin)
            for _ in range(5):
                raw = await asyncio.wait_for(ws.recv(), timeout=10)
                if isinstance(raw, str):
                    raw = raw.encode()
                body = raw[4:] if len(raw) > 4 and struct.unpack(">I", raw[:4])[0] == len(raw) - 4 else raw
                resp = decode_response(body)
                tid = resp.get(154467, [None])[0]
                if tid == 11:
                    break

            rp_codes   = resp.get(132766, [])
            uid        = resp.get(153428, [])
            user_msgs  = resp.get(132760, [])

            print(f"\n--- Response ---")
            print(f"  template_id    : {resp.get(154467)}")
            print(f"  rp_code        : {rp_codes}")
            print(f"  user_msg       : {user_msgs}")
            print(f"  unique_user_id : {uid}")

            if "0" in rp_codes:
                print("\n  *** LOGIN OK ***")
                return True
            elif "13" in rp_codes:
                print("\n  *** LOGIN FAILED: rp_code=13 (bad credentials / account locked) ***")
            else:
                print(f"\n  *** LOGIN FAILED: rp_code={rp_codes} ***")

    except asyncio.TimeoutError:
        print("ERROR: Timed out waiting for login response")
    except Exception as e:
        print(f"ERROR: {e}")

    return False


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--user",     required=True)
    p.add_argument("--password", required=True)
    p.add_argument("--system",   default="Rithmic Paper Trading")
    p.add_argument("--plant",    default="ORDER_PLANT", choices=list(INFRA))
    p.add_argument("--url",      default=RITHMIC_URL)
    args = p.parse_args()

    ok = asyncio.run(test_login(args.user, args.password, args.system, args.plant, args.url))
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
