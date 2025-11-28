import grpc
from concurrent import futures
import time
import transactions_pb2
import transactions_pb2_grpc

class TransactionServicer(transactions_pb2_grpc.TransactionServiceServicer):
    def StreamTransactions(self, request_iterator, context):
        for tx in request_iterator:
            print(f"Received gRPC Transaction: {tx.TransactionID}, Amount: {tx.TransactionAmt}")
        return transactions_pb2.Ack(message="Transactions received")

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    transactions_pb2_grpc.add_TransactionServiceServicer_to_server(TransactionServicer(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    print("gRPC server listening on 50051")
    server.wait_for_termination()

if __name__ == "__main__":
    serve()
