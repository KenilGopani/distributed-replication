import grpc
from concurrent import futures
import time
import argparse
import threading
import replication_pb2
import replication_pb2_grpc
import random
from collections import deque

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
        self.threshold = 1
        self.max_hops = 5
        self.last_task_time = time.time()
        # Weights for rank equation
        self.c0, self.c1, self.c2, self.c3 = 1.0, -1.0, -0.5, 0.2
        # Local queue of pending tasks
        self.task_queue = deque()
        # Start local task processor thread
        processor_thread = threading.Thread(target=self.process_tasks, daemon=True)
        processor_thread.start()

    def calculate_rank(self, server):
        """Compute rank score for a server based on factors."""
        load = self.server_metrics.get(server, 0)
        x0 = max(0, self.threshold - load)
        x1 = load
        x2 = self.max_hops
        x3 = time.time() - self.last_task_time
        return self.c0*x0 + self.c1*x1 + self.c2*x2 + self.c3*x3

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
        # Determine if forwarded to avoid recursive queries
        is_forwarded = any(md.key == 'forwarded' and md.value == 'true' for md in context.invocation_metadata())
        # Check local store
        with self.lock:
            if request.key in self.data_store:
                rec = self.data_store[request.key]
                print(f"Read: local hit {request.key} -> {rec['data']}")
                if is_forwarded:
                    # For forwarded reads, return immediately
                    return replication_pb2.ReadResponse(status="Success", data=rec['data'], timestamp=rec['timestamp'])
                # For original requests, start with local response
                responses = [{"data": rec['data'], "timestamp": rec['timestamp']}]
            else:
                if is_forwarded:
                    # Key not found locally on forwarded read
                    return replication_pb2.ReadResponse(status="Failed: Key not found")
                responses = []

        # 2) peer reads for original requests until R=2 fulfilled
        if not is_forwarded:
            peers = [p for p in self.server_metrics.keys() if p != self.server_id]
            random.shuffle(peers)
            for peer in peers:
                if len(responses) >= 2:
                    break
                try:
                    with grpc.insecure_channel(peer) as channel:
                        stub = replication_pb2_grpc.ReplicationStub(channel)
                        resp = stub.Read(request, timeout=5, metadata=[('forwarded','true')])
                        if resp.status == 'Success':
                            responses.append({"data": resp.data, "timestamp": resp.timestamp})
                            print(f"Read: got from peer {peer}: {resp.data}")
                except grpc.RpcError as e:
                    print(f"Read: peer {peer} error {e}")
        # Enforce R>=2 quorum
        if len(responses) >= 2:
            best = max(responses, key=lambda r: r['timestamp'])
            print(f"Read: returning latest {request.key} -> {best['data']}")
            return replication_pb2.ReadResponse(status="Success", data=best['data'], timestamp=best['timestamp'])
        # Quorum not met
        print(f"Read: insufficient responses {len(responses)}/2 for key {request.key}")
        return replication_pb2.ReadResponse(status=f"Failed: quorum {len(responses)}/2")

    def HandleTask(self, request, context):
        print(f"Incoming task {request.task_id} at {self.server_id}")
        # choose best server by rank
        scores = {srv: self.calculate_rank(srv) for srv in self.server_metrics}
        best = max(scores, key=scores.get)
        if best != self.server_id:
            print(f"Forwarding task {request.task_id} to {best}")
            with grpc.insecure_channel(best) as ch:
                stub = replication_pb2_grpc.ReplicationStub(ch)
                return stub.HandleTask(request)
        # enqueue locally
        with self.lock:
            self.task_queue.append(request)
        print(f"Queued task {request.task_id} locally")
        return replication_pb2.TaskResponse(status="Queued")

    def process_tasks(self):
        """Background thread: process tasks from local queue"""
        while True:
            with self.lock:
                if self.task_queue:
                    task = self.task_queue.popleft()
                else:
                    task = None
            if task:
                self.last_task_time = time.time()
                print(f"Processing queued task {task.task_id} locally")
                time.sleep(0.5)
            else:
                time.sleep(0.1)

    def share_load_with_peers(self):
        """Periodically share queue length with peers."""
        while True:
            with self.lock:
                local_load = len(self.task_queue)
                for peer in self.server_metrics.keys():
                    if peer != self.server_id:
                        try:
                            with grpc.insecure_channel(peer) as channel:
                                stub = replication_pb2_grpc.ReplicationStub(channel)
                                request = replication_pb2.LoadReport(server_id=self.server_id, load=local_load)
                                response = stub.ShareLoad(request)
                                if response.status != "Success":
                                    print(f"Failed to share load with peer {peer}")
                        except Exception as e:
                            print(f"Error sharing load with peer {peer}: {str(e)}")
            time.sleep(5)

    def redistribute_tasks(self):
        """Periodically steal tasks from most loaded peer when imbalance exceeds threshold."""
        while True:
            with self.lock:
                if self.server_metrics:
                    most_loaded = max(self.server_metrics, key=self.server_metrics.get)
                    least_loaded = min(self.server_metrics, key=self.server_metrics.get)
                    load_diff = self.server_metrics[most_loaded] - self.server_metrics[least_loaded]
                else:
                    load_diff = 0
            if load_diff > 2:
                print(f"Attempting to steal task from {most_loaded} (diff={load_diff})")
                try:
                    with grpc.insecure_channel(most_loaded) as channel:
                        stub = replication_pb2_grpc.ReplicationStub(channel)
                        # ask peer to redistribute one task
                        response = stub.RedistributeTask(replication_pb2.TaskRequest(task_id=""), timeout=5)
                        status = response.status
                        if status.startswith("STOLEN:"):
                            stolen_id = status.split("STOLEN:")[1]
                            with self.lock:
                                self.task_queue.append(replication_pb2.TaskRequest(task_id=stolen_id))
                            print(f"Stole task {stolen_id} from {most_loaded}")
                        else:
                            print(f"No task to steal from {most_loaded}")
                except Exception as e:
                    print(f"Error stealing task: {str(e)}")
            time.sleep(10)

    def RedistributeTask(self, request, context):
        """Handle task steal requests: pop one queued task and return its ID"""
        with self.lock:
            if self.task_queue:
                stolen = self.task_queue.popleft()
                print(f"Redistributing task {stolen.task_id} to peer")
                return replication_pb2.TaskResponse(status=f"STOLEN:{stolen.task_id}")
        return replication_pb2.TaskResponse(status="NONE")

    def ShareLoad(self, request, context):
        """Handle load sharing between peers."""
        with self.lock:
            self.server_metrics[request.server_id] = request.load
            print(f"Received load update from {request.server_id}: {request.load}")
        return replication_pb2.LoadResponse(status="Success")

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

    # Start local task processor for queued tasks
    task_processor_thread = threading.Thread(target=servicer.process_tasks, daemon=True)
    task_processor_thread.start()
    # Start hinted-handoff retry thread for failure recovery
    hint_thread = threading.Thread(target=servicer.process_hints, daemon=True)
    hint_thread.start()

    server.add_insecure_port(f'0.0.0.0:{args.port}')
    server.start()
    print(f"Server started and listening on port {args.port}")
    server.wait_for_termination()

if __name__ == "__main__":
    serve()