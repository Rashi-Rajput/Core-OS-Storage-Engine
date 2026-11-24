import os
from pathlib import Path
import select
import socket
import subprocess
import tempfile
import time
import unittest

BINARY = Path(os.environ.get("COREOS_BIN", Path(__file__).resolve().parents[1] / "coreos")).resolve()


class Client:
    def __init__(self, port):
        self.sock = socket.create_connection(("127.0.0.1", port), timeout=4)
        self.reader = self.sock.makefile("rb")

    def send(self, sql):
        self.sock.sendall((sql + "\n").encode())

    def reply(self):
        lines = []
        while True:
            line = self.reader.readline()
            if not line:
                raise AssertionError("connection closed before reply")
            lines.append(line.decode().rstrip("\n"))
            if lines[-1].startswith(("OK", "ERROR")):
                return "\n".join(lines)

    def ask(self, sql):
        self.send(sql)
        return self.reply()

    def blocked(self):
        return not select.select([self.sock], [], [], 0.25)[0]

    def close(self):
        self.reader.close()
        self.sock.close()


class EngineTest(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.folder = Path(tmp.name)
        self.log = self.folder / "stdout"
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            self.port = sock.getsockname()[1]
        self.clients = []
        self.process = None
        self.addCleanup(self.stop)
        self.start()

    def start(self):
        with self.log.open("w") as log:
            self.process = subprocess.Popen([BINARY, str(self.port)], cwd=self.folder,
                                            stdout=log, stderr=subprocess.STDOUT)
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline and self.process.poll() is None:
            if "server listening" in self.log.read_text():
                return
            time.sleep(0.02)
        self.fail(self.log.read_text() or "startup timed out")

    def stop(self):
        if self.process:
            self.process.kill()
            self.process.wait(timeout=4)
            self.process = None
        for client in self.clients:
            client.close()
        self.clients.clear()

    def connect(self):
        client = Client(self.port)
        self.clients.append(client)
        return client

    def ok(self, client, sql):
        result = client.ask(sql)
        self.assertTrue(result.splitlines()[-1].startswith("OK"), result)
        return result
