import asyncio
import enum

from .request import RequestIterator, ResponseMessageStream


class RequestData(object):
    def __init__(self, method, headers, *,
                 iterator=None,
                 request_stream=None,
                 response_stream=None,
                 future=None):
        self.method = method
        self.headers = headers
        self.iterator = iterator
        self.request_stream = request_stream
        self.response_stream = response_stream
        self.future = future


class Cardinality(enum.Enum):
    UNARY_UNARY = 1
    UNARY_STREAM = 2
    STREAM_UNARY = 3
    STREAM_STREAM = 4


class ServiceMethod(object):
    def __init__(self, fn, request, response, cardinality=Cardinality.UNARY_UNARY):
        self.fn = fn
        self.request = request
        self.response = response
        self.cardinality = cardinality

    @classmethod
    def decorator(cls, *args, **kwargs):
        def decorator(fn):
            return ServiceMethod(fn, *args, **kwargs)

        return decorator

    async def invoke(self, request, *args, **kwargs):
        if asyncio.iscoroutinefunction(self.fn):
            return await self.fn(request, *args, **kwargs)
        else:
            return self.fn(request, *args, **kwargs)
