# ------------------------------
# Makefile for gRPC Python generation and running server/client
# ------------------------------

# Proto file
PROTO_FILE = transactions.proto

# Output directories
PYTHON_OUT = .
GRPC_PYTHON_OUT = .

# Default target: generate gRPC Python files
all: grpc

# Generate gRPC Python code
grpc:
	@echo "Generating gRPC Python files from $(PROTO_FILE)..."
	python -m grpc_tools.protoc -I. --python_out=$(PYTHON_OUT) --grpc_python_out=$(GRPC_PYTHON_OUT) $(PROTO_FILE)
	@echo "Done."

# Clean generated files
clean:
	@echo "Cleaning generated gRPC files..."
	rm -f $(PYTHON_OUT)/transactions_pb2.py
	rm -f $(GRPC_PYTHON_OUT)/transactions_pb2_grpc.py
	@echo "Done."

# Run gRPC server
run-server:
	python grpc_server.py

# Run gRPC client
run-client:
	python grpc_client.py
