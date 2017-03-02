from __future__ import print_function

from time import sleep

import grpc

import greeter_pb2


def _name_generator():
    names = ('Foo', 'Bar', 'Bat', 'Baz')

    for name in names:
        yield greeter_pb2.HelloRequest(name=name)
        sleep(0.5)


if __name__ == '__main__':
    channel = grpc.insecure_channel('127.0.0.1:50051')
    stub = greeter_pb2.GreeterStub(channel)

    response = stub.SayHello(greeter_pb2.HelloRequest(name='you'))
    print("Greeter client received: " + response.message)

    response_iterator = stub.SayHelloGoodbye(greeter_pb2.HelloRequest(name="y'all"))

    for response in response_iterator:
        print(response.message)

    response_iterator = stub.SayHelloToMany(_name_generator())

    for response in response_iterator:
        print(response.message)

    response = stub.SayHelloToManyAtOnce(_name_generator())
    print(response.message)
