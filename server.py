import grpc
from concurrent import futures
import time
import argparse
import threading
import replication_pb2
import replication_pb2_grpc

class ReplicationServicer(replication_pb2_grpc.ReplicationServicer):
    def __init__(self, server_id, ip_address):
        self.server_id = f"{ip_address}:{server_id.split(':')[1]}"
        self.data_store = {}  # Key-value store for data
        self.lock = threading.Lock()
        self.load = 0  # Track the number of tasks being processed

    def Write(self, request, context):
        with self.lock:
            self.data_store[request.key] = {
                "data": request.data,
                "timestamp": time.time()
            }
            self.load += 1
            print(f"Data written: {request.key} -> {request.data}")
        time.sleep(1)  # Simulate processing time
        with self.lock:
            self.load -= 1
        return replication_pb2.WriteResponse(status="Success")

    def Read(self, request, context):
        with self.lock:
            if request.key in self.data_store:
                record = self.data_store[request.key]
                print(f"Data read: {request.key} -> {record['data']}")
                return replication_pb2.ReadResponse(
                    status="Success",
                    data=record["data"],
                    timestamp=record["timestamp"]
                )
            else:
                print(f"Data not found for key: {request.key}")
                return replication_pb2.ReadResponse(status="Failed: Key not found")

    def HandleTask(self, request, context):
        print(f"Processing task with ID: {request.task_id}")
        # Simulate task processing
        time.sleep(0.5)  # Simulate a delay for task processing
        return replication_pb2.TaskResponse(status="Success")

    def report_load_to_coordinator(self):
        while True:
            try:
                with grpc.insecure_channel('10.0.0.223:50055') as channel:  # Ensure correct coordinator address
                    stub = replication_pb2_grpc.ReplicationStub(channel)
                    request = replication_pb2.LoadReport(server_id=self.server_id, load=self.load)
                    response = stub.ReportLoad(request)
                    if response.status != "Success":
                        print(f"Failed to report load for server {self.server_id}")
            except grpc.RpcError as e:
                print(f"Error reporting load for server {self.server_id}: {str(e)}")
            time.sleep(5)  # Report load every 5 seconds

def serve():
    parser = argparse.ArgumentParser(description="Start a replication server.")
    parser.add_argument('--port', type=int, default=50051, help='Port number for the server')
    parser.add_argument('--ip', type=str, default='127.0.0.1', help='IP address of the server')
    args = parser.parse_args()

    server_id = f"{args.ip}:{args.port}"
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    servicer = ReplicationServicer(server_id, args.ip)
    replication_pb2_grpc.add_ReplicationServicer_to_server(servicer, server)

    # Start load reporting thread
    load_reporting_thread = threading.Thread(target=servicer.report_load_to_coordinator, daemon=True)
    load_reporting_thread.start()

    server.add_insecure_port(f'0.0.0.0:{args.port}')
    server.start()
    print(f"Server started and listening on port {args.port}")
    server.wait_for_termination()

if __name__ == "__main__":
    serve()