import asyncio
import collections
import struct
from typing import Dict, List, Tuple

import logging
from h2.connection import H2Connection
from h2.errors import PROTOCOL_ERROR
from h2.events import RequestReceived, DataReceived, StreamEnded

from h2_grpc_server import RequestIterator, ResponseMessageStream, RequestData, Cardinality

logger = logging.getLogger(__name__)


class H2Protocol(asyncio.Protocol):
    def __init__(self, methods: Dict[str, 'ServiceMethod'], *, loop):
        self._loop = loop
        self._events = asyncio.Queue()
        self.conn = H2Connection(client_side=False)
        self.transport = None
        self.stream_data = {}
        self.methods = methods

    def connection_made(self, transport: asyncio.Transport):
        self.transport = transport
        self.conn.initiate_connection()
        self.transport.write(self.conn.data_to_send())

    def _unary_unary(self):
        pass

    def _stream_stream(self):
        pass

    async def _data_received(self, data: bytes):
        events = self.conn.receive_data(data)
        for event in events:
            logger.info('EVENT {}'.format(event))
            if isinstance(event, RequestReceived):
                self.request_received(event.headers, event.stream_id)
            elif isinstance(event, DataReceived):
                logger.info("Data #1: {}".format(event.data))
                self.receive_data(event.data, event.stream_id)

            elif isinstance(event, StreamEnded):
                logger.info('STREAM ENDED {}'.format(event))

                try:
                    request_data = self.stream_data[event.stream_id]
                except KeyError:
                    # Just return, we probably 405'd this already
                    return

                self.stream_data[event.stream_id].request_stream.close()

    def data_received(self, data: bytes):
        asyncio.ensure_future(self._data_received(data), loop=self._loop)

    def _init_stream_stream(self, request):
        request.iterator = RequestIterator(request.method.request)
        request.request_stream = request.iterator.stream
        request.response_stream = ResponseMessageStream()

        async def _invoke_and_return():
            await request.method.invoke(request.request_stream, request.response_stream)
            request.request_stream.close()  # in case it wasn't closed yet.
            request.response_stream.close()

        request.future = asyncio.ensure_future(_invoke_and_return())

    def _init_unary_unary(self, request):
        request.iterator = RequestIterator(request.method.request)
        request.request_stream = request.iterator.stream
        request.response_stream = ResponseMessageStream()

        async def _invoke_and_return():
            request_message = await request.request_stream.receive()
            request.request_stream.close()
            response_message = await request.method.invoke(request_message)
            request.response_stream.send(response_message)
            request.response_stream.close()

        request.future = asyncio.ensure_future(_invoke_and_return())

    def _init_unary_stream(self, request):
        request.iterator = RequestIterator(request.method.request)
        request.request_stream = request.iterator.stream
        request.response_stream = ResponseMessageStream()

        async def _invoke_and_return():
            request_message = await request.request_stream.receive()
            request.request_stream.close()
            await request.method.invoke(request_message, request.response_stream)
            request.response_stream.close()

        request.future = asyncio.ensure_future(_invoke_and_return())

    def _init_stream_unary(self, request):
        request.iterator = RequestIterator(request.method.request)
        request.request_stream = request.iterator.stream
        request.response_stream = ResponseMessageStream()

        async def _invoke_and_return():
            response_message = await request.method.invoke(request.request_stream)
            request.response_stream.send(response_message)
            request.response_stream.close()

        request.future = asyncio.ensure_future(_invoke_and_return())

    def request_received(self, headers: List[Tuple[str, str]], stream_id: int):
        headers = collections.OrderedDict(headers)
        http_method = headers[':method']
        http_path = headers[':path']

        logger.info(repr(headers))

        if http_path not in self.methods:
            self.return_404(headers, stream_id)
            return

        # We only support GET and POST.
        if http_method not in ('GET', 'POST',):
            self.return_405(headers, stream_id)
            return

        # TODO start timeout timer
        method = self.methods[http_path]

        # Store off the request data.
        request_data = RequestData(method, headers)
        self.stream_data[stream_id] = request_data

        if request_data.method.cardinality == Cardinality.UNARY_UNARY:
            self._init_unary_unary(request_data)
        elif request_data.method.cardinality == Cardinality.UNARY_STREAM:
            self._init_unary_stream(request_data)
        elif request_data.method.cardinality == Cardinality.STREAM_UNARY:
            self._init_stream_unary(request_data)
        elif request_data.method.cardinality == Cardinality.STREAM_STREAM:
            self._init_stream_stream(request_data)

        request_data.response_future = asyncio.ensure_future(self.response_from_stream(request_data, stream_id))

    async def response_from_stream(self, request_data: RequestData, stream_id: int):
        self.conn.send_headers(stream_id, (
            (':status', '200'),
            ('content-type', 'application/grpc+proto'),
            ('server', 'asyncio-h2-grpc'),
        ), end_stream=False)

        async for message in request_data.response_stream:
            logger.info("RESPONSE MESSAGE {}".format(message))

            response_message_body = message.SerializeToString()
            response_data = struct.pack('?', False) + \
                            struct.pack('>I', len(response_message_body)) + \
                            response_message_body

            self.conn.send_data(stream_id, response_data, end_stream=False)
            self.transport.write(self.conn.data_to_send())

        logger.info('STREAM DONE')

        self.conn.send_headers(stream_id, (
            ('grpc-status', '0'),
        ), end_stream=True)

        self.transport.write(self.conn.data_to_send())

    def return_404(self, headers: List[Tuple[str, str]], stream_id: int):
        """
        We don't support the given method, so we want to return a 405 response.
        """
        response_headers = (
            (':status', '405'),
            ('content-length', '0'),
            ('server', 'asyncio-h2'),
        )
        self.conn.send_headers(stream_id, response_headers, end_stream=True)

    def return_405(self, headers: List[Tuple[str, str]], stream_id: int):
        """
        We don't support the given method, so we want to return a 405 response.
        """
        response_headers = (
            (':status', '405'),
            ('content-length', '0'),
            ('server', 'asyncio-h2'),
        )
        self.conn.send_headers(stream_id, response_headers, end_stream=True)

    def receive_data(self, data: bytes, stream_id: int):
        """
        We've received some data on a stream. If that stream is one we're
        expecting data on, save it off. Otherwise, reset the stream.
        """
        try:
            stream_data = self.stream_data[stream_id]
        except KeyError:
            self.conn.reset_stream(stream_id, error_code=PROTOCOL_ERROR)
        else:
            stream_data.iterator.write(data)