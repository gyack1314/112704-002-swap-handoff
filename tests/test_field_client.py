"""检查联调会话的证据文件、TCP 帧及断线。"""

import base64
from hashlib import sha256
import io
from pathlib import Path
import socketserver
import sys
from threading import Thread
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from field_client import encode, load_session, receive, send_session


class Receiver(socketserver.StreamRequestHandler):
    def handle(self):
        while (message := receive(self.rfile)) is not None:
            self.wfile.write(encode({'original': message, 'peer_port': self.client_address[1]}))


class SessionTests(unittest.TestCase):
    def test_published_session_crosses_tcp(self):
        messages = load_session(Path(__file__).resolve().parents[1] / 'field-session.json')
        with socketserver.ThreadingTCPServer(('127.0.0.1', 0), Receiver) as server:
            worker = Thread(target=server.serve_forever, kwargs={'poll_interval': 0.02})
            worker.start()
            try:
                replies = list(send_session(messages, *server.server_address))
            finally:
                server.shutdown()
                worker.join()
        active = [message for message in messages if message['kind'] != 'disconnect']
        self.assertEqual([reply['original'] for reply in replies], active)
        disconnects = [row for row in messages if row['kind'] == 'disconnect']
        self.assertTrue(disconnects)
        for disconnected in disconnects:
            actor, at = disconnected['actor'], disconnected['at']
            prior = [r['peer_port'] for r in replies if r['original']['actor'] == actor and r['original']['at'] < at]
            later = [r['peer_port'] for r in replies if r['original']['actor'] == actor and r['original']['at'] > at]
            self.assertTrue(prior and later, actor)
            self.assertNotEqual(prior[-1], later[0], actor)
        for row in active:
            body = row['body']
            if 'raw_b64' in body:
                raw = base64.b64decode(body['raw_b64'], validate=True)
                self.assertEqual(sha256(raw).hexdigest(), body['sha256'])
                self.assertEqual(len(raw), body['bytes'][1] - body['bytes'][0])

    def test_truncated_frame_cannot_be_read_as_message(self):
        full = encode({'kind': '时序'})
        with self.assertRaises(EOFError):
            receive(io.BytesIO(full[:-2]))


if __name__ == '__main__':
    unittest.main()
