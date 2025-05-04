import grpc
from concurrent import futures
import time
import argparse
import threading
import replication_pb2
import replication_pb2_grpc
import random

class ReplicationServicer(replication_pb2_grpc.ReplicationServicer):
    def __init__(self, server_id, ip_address):
        self.server_id = f"{ip_address}:{server_id.split(':')[1]}"
        self.data_store = {}  # Key-value store for data
        self.lock = threading.Lock()
        self.load = 0  # Track the number of tasks being processed
        # Initialize server metrics with all 5 peer addresses
        self.server_metrics = {
            "127.0.0.1:50051": 0,
            "127.0.0.1:50052": 0,
            "127.0.0.1:50053": 0,
            "127.0.0.1:50054": 0,
            "127.0.0.1:50055": 0
        }
        # Hinted-handoff queue for failed write replications
        self.hints = {peer: [] for peer in self.server_metrics.keys()}

    def Write(self, request, context):
        """Handle write requests with logging to debug peer communication."""
        # Detect forwarded writes to avoid replication loops
        is_forwarded = any(md.key == 'forwarded' and md.value == 'true' for md in context.invocation_metadata())
        with self.lock:
            self.data_store[request.key] = {
                "data": request.data,
                "timestamp": time.time()
            }
            self.load += 1
            print(f"Data written locally: {request.key} -> {request.data}")

        # Log the receipt of the write request
        print(f"Received Write request for key: {request.key} with data: {request.data}")

        # Replicate write to two random peers only for original requests
        if not is_forwarded:
            peers = [p for p in self.server_metrics.keys() if p != self.server_id]
            selected = random.sample(peers, min(2, len(peers)))
            for peer in selected:
                try:
                    with grpc.insecure_channel(peer) as channel:
                        stub = replication_pb2_grpc.ReplicationStub(channel)
                        # Mark this write as forwarded to prevent re-replication
                        stub.Write(request, timeout=5, metadata=[('forwarded', 'true')])
                        print(f"Replicated write for key {request.key} to peer {peer}")
                except grpc.RpcError as e:
                    print(f"Error replicating write to peer {peer}: {str(e)} - queued for retry")
                    # Queue hint for retry when peer recovers
                    with self.lock:
                        self.hints[peer].append(replication_pb2.WriteRequest(key=request.key, data=request.data))

        # Simulate processing time
        time.sleep(0.5)

        with self.lock:
            self.load -= 1

        return replication_pb2.WriteResponse(status="Success")

    def Read(self, request, context):
        """Handle read requests with logging to debug peer communication."""
        # Determine if this request is forwarded to avoid recursive peer queries
        is_forwarded = any(md.key == 'forwarded' and md.value == 'true' for md in context.invocation_metadata())

        # Query self first and return immediately if found
        with self.lock:
            if request.key in self.data_store:
                record = self.data_store[request.key]
                print(f"Read request: Found key {request.key} locally with data: {record['data']}")
                return replication_pb2.ReadResponse(
                    status="Success",
                    data=record['data'],
                    timestamp=record['timestamp']
                )

        # Prepare to collect peer responses for keys not found locally
        responses = []
        # Query peers only for original requests, skip for forwarded
        if not is_forwarded:
            for peer in self.server_metrics.keys():
                if peer != self.server_id:  # Avoid querying itself
                    try:
                        with grpc.insecure_channel(peer) as channel:
                            stub = replication_pb2_grpc.ReplicationStub(channel)
                            # Forwarded reads include metadata to avoid re-querying
                            response = stub.Read(request, timeout=5, metadata=[('forwarded', 'true')])  
                            if response.status == "Success":
                                responses.append({"data": response.data, "timestamp": response.timestamp})
                                print(f"Read request: Received data from peer {peer} for key {request.key}.")
                    except grpc.RpcError as e:
                        print(f"Error reading from peer {peer}: {str(e)}")

        # Reconcile peer responses
        if responses:
            latest = max(responses, key=lambda r: r['timestamp'])
            print(f"Read request: Returning latest data for key {request.key}: {latest['data']}")
            return replication_pb2.ReadResponse(
                status="Success",
                data=latest['data'],
                timestamp=latest['timestamp']
            )
        # No data found locally or on peers
        print(f"Read request: Key {request.key} not found in any server.")
        return replication_pb2.ReadResponse(status="Failed: Key not found")

    def HandleTask(self, request, context):
        print(f"Processing task with ID: {request.task_id}")
        # Simulate task processing
        time.sleep(0.5)  # Simulate a delay for task processing
        return replication_pb2.TaskResponse(status="Success")

    def ShareLoad(self, request, context):
        """Handle load sharing between peers."""
        with self.lock:
            self.server_metrics[request.server_id] = request.load
            print(f"Received load update from {request.server_id}: {request.load}")
        return replication_pb2.LoadResponse(status="Success")

    def RedistributeTask(self, request, context):
        """Handle task redistribution from peers."""
        print(f"Received task redistribution request: {request.task_id}")
        # Simulate task processing
        time.sleep(0.5)
        return replication_pb2.TaskResponse(status="Success")

    def share_load_with_peers(self):
        """Periodically share load information with peers."""
        while True:
            with self.lock:
                for peer in self.server_metrics.keys():
                    if peer != self.server_id:  # Avoid sending to itself
                        try:
                            with grpc.insecure_channel(peer) as channel:
                                stub = replication_pb2_grpc.ReplicationStub(channel)
                                request = replication_pb2.LoadReport(server_id=self.server_id, load=self.load)
                                response = stub.ShareLoad(request)
                                if response.status != "Success":
                                    print(f"Failed to share load with peer {peer}")
                        except grpc.RpcError as e:
                            print(f"Error sharing load with peer {peer}: {str(e)}")
                        except Exception as e:
                            print(f"Unexpected error while sharing load with peer {peer}: {str(e)}")
            time.sleep(5)  # Share load every 5 seconds

    def redistribute_tasks(self):
        """Periodically check for imbalances and redistribute tasks."""
        while True:
            with self.lock:
                # Find the most and least loaded servers
                if self.server_metrics:
                    most_loaded = max(self.server_metrics, key=self.server_metrics.get)
                    least_loaded = min(self.server_metrics, key=self.server_metrics.get)

                    # Redistribute tasks if imbalance exceeds a threshold
                    if self.server_metrics[most_loaded] - self.server_metrics[least_loaded] > 2:  # Example threshold
                        print(f"Redistributing tasks from {most_loaded} to {least_loaded}")
                        try:
                            with grpc.insecure_channel(most_loaded) as channel:
                                stub = replication_pb2_grpc.ReplicationStub(channel)
                                request = replication_pb2.TaskRequest(task_id="redistributed_task")
                                response = stub.RedistributeTask(request)
                                if response.status != "Success":
                                    print(f"Failed to redistribute task from {most_loaded} to {least_loaded}")
                        except grpc.RpcError as e:
                            print(f"Error redistributing task: {str(e)}")
                        except Exception as e:
                            print(f"Unexpected error while redistributing task: {str(e)}")
            time.sleep(10)  # Check for imbalances every 10 seconds

    def process_hints(self):
        """Background thread: retry hinted writes to peers that were previously unreachable."""
        while True:
            with self.lock:
                for peer, queue in list(self.hints.items()):
                    remaining = []
                    for hint_req in queue:
                        try:
                            with grpc.insecure_channel(peer) as channel:
                                stub = replication_pb2_grpc.ReplicationStub(channel)
                                stub.Write(hint_req, timeout=5, metadata=[('forwarded','true')])
                                print(f"Replayed hinted write for key {hint_req.key} to {peer}")
                        except grpc.RpcError:
                            remaining.append(hint_req)
                    self.hints[peer] = remaining
            time.sleep(5)

def serve():
    parser = argparse.ArgumentParser(description="Start a replication server.")
    parser.add_argument('--port', type=int, default=50051, help='Port number for the server')
    parser.add_argument('--ip', type=str, default='127.0.0.1', help='IP address of the server')
    args = parser.parse_args()

    server_id = f"{args.ip}:{args.port}"
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    servicer = ReplicationServicer(server_id, args.ip)
    replication_pb2_grpc.add_ReplicationServicer_to_server(servicer, server)

    # Start load sharing thread
    load_sharing_thread = threading.Thread(target=servicer.share_load_with_peers, daemon=True)
    load_sharing_thread.start()

    # Start task redistribution thread
    task_redistribution_thread = threading.Thread(target=servicer.redistribute_tasks, daemon=True)
    task_redistribution_thread.start()

    # Start hinted-handoff retry thread for failure recovery
    hint_thread = threading.Thread(target=servicer.process_hints, daemon=True)
    hint_thread.start()

    server.add_insecure_port(f'0.0.0.0:{args.port}')
    server.start()
    print(f"Server started and listening on port {args.port}")
    server.wait_for_termination()

if __name__ == "__main__":
    serve()