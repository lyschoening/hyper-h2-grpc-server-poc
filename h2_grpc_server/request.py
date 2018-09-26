import asyncio
from abc import ABC, abstractmethod


import struct

try:
    from socket import socketpair
except ImportError:
    from asyncio.windows_utils import socketpair


class MessageStream(ABC):
    def __aiter__(self):
        return self

    async def __anext__(self):
        message = await self.receive()
        if message is None:
            raise StopAsyncIteration
        return message

    @abstractmethod
    def close(self):
        pass

    @abstractmethod
    async def receive(self):
        pass


class ResponseMessageStream(MessageStream):
    def __init__(self):
        self.messages = []
        self._event = asyncio.Event()
        self.is_closed = False

    def send(self, message):

        if self.is_closed:
            raise Exception('Stream is closed')
        self.messages.append(message)
        self._event.set()

    def close(self):
        self.is_closed = True
        self._event.set()

    async def receive(self):
        while True:
            if self.messages:
                return self.messages.pop(0)
            elif self.is_closed:
                return None
            else:
                await self._event.wait()
                self._event.clear()


class RequestMessageStream(MessageStream):
    def __init__(self, iterator: 'RequestIterator'):
        self._request_iterator = iterator

    def close(self):
        self._request_iterator.close()

    async def receive(self):
        return await self._request_iterator.receive_message()


class RequestIterator(object):
    def __init__(self, message_cls, encoding=None):
        self._message_cls = message_cls
        self._message_stream = RequestMessageStream(self)
        self._encoding = encoding
        self._data = asyncio.Queue()
        self._buffer = bytearray()
        self._buffer_write = asyncio.Event()
        self._is_closed = False

    def write(self, data: bytes):
        self._buffer.extend(data)
        self._buffer_write.set()

    def close(self):
        self._is_closed = True
        self._buffer_write.set()

    async def _read(self):
        await self._buffer_write.wait()
        self._buffer_write.clear()
        if self._is_closed:
            return False
        return True

    async def receive_message(self):
        buffer = self._buffer

        while len(self._buffer) < 5:
            if not await self._read():
                return  # TODO exception

        compressed_flag = struct.unpack('?', self._buffer[0:1])[0]
        message_length = struct.unpack('>I', self._buffer[1:5])[0]

        # TODO handle compression
        if compressed_flag:
            raise NotImplementedError

        self._buffer = buffer = buffer[5:]
        while len(buffer) < message_length:
            if not await self._read():
                return  # TODO exception

        message = self._message_cls()
        message.ParseFromString(bytes(buffer[0:message_length]))
        self._buffer = buffer[message_length:]
        return message

    @property
    def stream(self):
        return self._message_stream
