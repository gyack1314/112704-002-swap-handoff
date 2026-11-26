"""本机实验端的消息收发。"""

import argparse
from contextlib import ExitStack
import json
from pathlib import Path
import socket
import struct

HEADER = struct.Struct('!I')
MAX_FRAME = 1024 * 1024


def encode(message):
    raw = json.dumps(message, ensure_ascii=False, separators=(',', ':'), allow_nan=False).encode('utf-8')
    if not 0 < len(raw) <= MAX_FRAME:
        raise ValueError('单帧负载超过联调范围')
    return HEADER.pack(len(raw)) + raw


def receive(source):
    header = source.read(HEADER.size)
    if not header:
        return None
    if len(header) != HEADER.size:
        raise EOFError('消息头尚未收齐')
    size, = HEADER.unpack(header)
    if not 0 < size <= MAX_FRAME:
        raise ValueError('无效的消息长度')
    raw = source.read(size)
    if len(raw) != size:
        raise EOFError('消息体尚未收齐')
    decoded = json.loads(raw)
    if not isinstance(decoded, dict):
        raise ValueError('联调消息必须为 JSON 对象')
    return decoded


def load_session(path):
    session = json.loads(path.read_text(encoding='utf-8'))
    if not isinstance(session['inventory'], dict) or not session['events']:
        raise ValueError('会话缺少设备登记或现场记录')
    messages = [dict(at=0, actor='registry', kind='inventory', body=session['inventory'])]
    for at, actor, kind, body in session['events']:
        if (type(at) is not int or at < messages[-1]['at'] or not isinstance(actor, str) or not actor
                or not isinstance(kind, str) or not kind or not isinstance(body, dict)):
            raise ValueError('会话记录的外壳或接收时点无效')
        messages.append(dict(at=at, actor=actor, kind=kind, body=body))
    return messages


def send_session(messages, host, port, timeout=3):
    with ExitStack() as all_connections:
        connections = {}
        for message in messages:
            actor = message['actor']
            if message['kind'] == 'disconnect':
                if actor in connections:
                    resources, _ = connections.pop(actor)
                    resources.close()
                continue
            if actor not in connections:
                resources = all_connections.enter_context(ExitStack())
                instance = resources.enter_context(socket.create_connection((host, port), timeout))
                stream = resources.enter_context(instance.makefile('rwb'))
                connections[actor] = resources, stream
            _, stream = connections[actor]
            stream.write(encode(message))
            stream.flush()
            reply = receive(stream)
            if reply is None:
                raise EOFError(f'{actor} 的连接未返回消息')
            yield reply


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    here = Path(__file__).with_name('field-session.json')
    parser.add_argument('session', nargs='?', default=here, type=Path)
    parser.add_argument('--check', action='store_true')
    parser.add_argument('--host', default='127.0.0.1')
    parser.add_argument('--port', type=int, default=8765)
    args = parser.parse_args()
    messages = load_session(args.session)
    if args.check:
        print(json.dumps({'消息': len(messages), '来源': sorted({msg['actor'] for msg in messages})}, ensure_ascii=False))
        return
    for reply in send_session(messages, args.host, args.port):
        print(json.dumps(reply, ensure_ascii=False))


if __name__ == '__main__':
    main()
