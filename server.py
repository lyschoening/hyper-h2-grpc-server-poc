import asyncio

import logging

from greeter_pb2 import HelloReply
from greeter_pb2 import HelloRequest
from h2_grpc_server import ResponseMessageStream
from h2_grpc_server import ServiceMethod, Cardinality
from h2_grpc_server.protocol import H2Protocol
from h2_grpc_server.request import RequestMessageStream


logging.basicConfig(level=logging.INFO)

@ServiceMethod.decorator(HelloRequest, HelloReply)
def say_hello(request):
    return HelloReply(message='Hello ' + request.name)


@ServiceMethod.decorator(HelloRequest, HelloReply, cardinality=Cardinality.UNARY_STREAM)
async def say_hello_goodbye(request: 'HelloRequest', response: 'ResponseMessageStream'):
    await asyncio.sleep(1)
    response.send(HelloReply(message='Hello ' + request.name))
    await asyncio.sleep(1)
    response.send(HelloReply(message='Goodbye ' + request.name))


@ServiceMethod.decorator(HelloRequest, HelloReply, cardinality=Cardinality.STREAM_STREAM)
async def say_hello_to_many(request: 'HelloRequest', response: 'ResponseMessageStream'):
    async for message in request:
        response.send(HelloReply(message='Hi ' + message.name))


@ServiceMethod.decorator(HelloRequest, HelloReply, cardinality=Cardinality.STREAM_UNARY)
async def say_hello_to_many_at_once(request: 'RequestMessageStream'):
    names = []
    async for message in request:
        names.append(message.name)

    return HelloReply(message='Hi ' + ', '.join(names) + '!')


loop = asyncio.get_event_loop()

if __name__ == '__main__':
    # Each client connection will create a new protocol instance
    coro = loop.create_server(lambda: H2Protocol({
        '/Greeter/SayHello': say_hello,
        '/Greeter/SayHelloGoodbye': say_hello_goodbye,
        '/Greeter/SayHelloToMany': say_hello_to_many,
        '/Greeter/SayHelloToManyAtOnce': say_hello_to_many_at_once
    }, loop=loop), '127.0.0.1', 50051)

    server = loop.run_until_complete(coro)

    print('Serving on {}'.format(server.sockets[0].getsockname()))
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass  # serve requests until Ctrl+C is pressed

    # Close the server
    server.close()
    loop.run_until_complete(server.wait_closed())
    loop.close()
