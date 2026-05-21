import shutil
import socket
import subprocess
import time
import pytest

MOSQUITTO_PORT = 11883


def _is_port_open(port: int, host: str = "127.0.0.1", timeout: float = 0.5) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


@pytest.fixture(scope="session")
def mosquitto_broker(tmp_path_factory):
    if not shutil.which("mosquitto"):
        pytest.skip("mosquitto not installed")
    conf = tmp_path_factory.mktemp("mosq") / "mosq.conf"
    conf.write_text(f"listener {MOSQUITTO_PORT}\nallow_anonymous true\n")
    proc = subprocess.Popen(
        ["mosquitto", "-c", str(conf)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    for _ in range(50):
        if _is_port_open(MOSQUITTO_PORT):
            break
        time.sleep(0.1)
    else:
        proc.kill()
        pytest.skip(f"mosquitto did not start on :{MOSQUITTO_PORT}")
    yield MOSQUITTO_PORT
    proc.terminate()
    proc.wait(timeout=5)
