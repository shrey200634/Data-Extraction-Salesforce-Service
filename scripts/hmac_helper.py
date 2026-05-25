"""Helper to generate HMAC signature for testing API endpoints."""
import hmac
import hashlib
import time
import sys

KEY = b"dummy-core-key-for-local-dev-xxxx"


def sign(method: str, path: str, body: str = ""):
    ts = str(int(time.time()))
    msg = f"{method}{path}{ts}{body}"
    sig = hmac.new(KEY, msg.encode(), hashlib.sha256).hexdigest()
    print(f"TIMESTAMP: {ts}")
    print(f"SIGNATURE: {sig}")
    return ts, sig


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python hmac_helper.py <METHOD> <PATH> [BODY]")
        sys.exit(1)
    method = sys.argv[1]
    path = sys.argv[2]
    body = sys.argv[3] if len(sys.argv) > 3 else ""
    sign(method, path, body)
