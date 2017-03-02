from grpc.tools import protoc

protoc.main(
    (
	'',
	'--python_out=.',
	'--grpc_python_out=.',
	'greeter.proto',
    )
)