import grpc
from concurrent import futures
import time
import threading
import random
import replication_pb2
import replication_pb2_grpc

class ReplicationServicer(replication_pb2_grpc.ReplicationServicer):
    def __init__(self):
        self.servers = [
            ('10.0.0.108', 50051),
            ('10.0.0.108', 50052),
            ('10.0.0.108', 50053),
            ('10.0.0.223', 50057),
            ('10.0.0.223', 50058)
        ]
        self.server_loads = {f"{ip}:{port}": 0 for ip, port in self.servers}
        self.active_servers = set(f"{ip}:{port}" for ip, port in self.servers)
        self.lock = threading.Lock()
        self.N = len(self.servers)  # Total number of servers
        self.W = 2  # Minimum writes for quorum
        self.R = 2  # Minimum reads for quorum
        self.server_metrics = {server: {"load": 0, "last_task_time": 0} for server in self.server_loads}
        self.threshold = 2  # Minimum load to disallow stealing
        self.max_hops = 5  # Placeholder for distance metric

    def calculate_rank(self, server):
        """Calculate the rank of a server based on multiple factors."""
        metrics = self.server_metrics[server]
        load = metrics["load"]
        last_task_time = metrics["last_task_time"]
        
        # Factors and weights
        c0, c1, c2, c3 = 1.0, -1.0, -0.5, 0.2
        x0 = max(0, self.threshold - load)  # Messages that can be stolen
        x1 = load  # Current load
        x2 = self.max_hops  # Distance (placeholder)
        x3 = time.time() - last_task_time  # Delay since last task

        # Rank equation
        rank = c0 * x0 + c1 * x1 + c2 * x2 + c3 * x3
        return rank

    def get_best_server(self):
        """Select the best server based on rank."""
        with self.lock:
            ranked_servers = {
                server: self.calculate_rank(server)
                for server in self.active_servers
            }
            return max(ranked_servers, key=ranked_servers.get, default=None)

    def get_least_loaded_server(self):
        with self.lock:
            available_servers = {server: load for server, load in self.server_loads.items() 
                              if server in self.active_servers}
            if not available_servers:
                return None
            
            # Find all servers with minimum load
            min_load = min(available_servers.values())
            least_loaded = [server for server, load in available_servers.items() 
                          if load == min_load]
            
            # Randomly select one of the least loaded servers
            return random.choice(least_loaded)

    def check_server_availability(self):
        while True:
            for ip, port in self.servers:
                server_addr = f"{ip}:{port}"
                try:
                    with grpc.insecure_channel(server_addr) as channel:
                        stub = replication_pb2_grpc.ReplicationStub(channel)
                        # Try a quick connection
                        grpc.channel_ready_future(channel).result(timeout=1)
                        with self.lock:
                            if server_addr not in self.active_servers:
                                print(f"Server {server_addr} is now available")
                                self.active_servers.add(server_addr)
                except (grpc.FutureTimeoutError, grpc.RpcError):
                    with self.lock:
                        if server_addr in self.active_servers:
                            print(f"Server {server_addr} is not responding")
                            self.active_servers.remove(server_addr)
            time.sleep(5)

    def HandleTask(self, request, context):
        print(f"Received task with ID: {request.task_id}")
        
        selected_server = self.get_best_server()
        if not selected_server:
            return replication_pb2.TaskResponse(status="No servers available")

        ip, port = selected_server.split(':')
        print(f"Attempting to forward task {request.task_id} to server {selected_server}")
        
        try:
            with grpc.insecure_channel(selected_server) as channel:
                stub = replication_pb2_grpc.ReplicationStub(channel)
                with self.lock:
                    self.server_metrics[selected_server]["load"] += 1
                    self.server_metrics[selected_server]["last_task_time"] = time.time()
                
                response = stub.HandleTask(request)
                print(f"Task {request.task_id} completed by server {selected_server}")
                
                with self.lock:
                    self.server_metrics[selected_server]["load"] -= 1
                
                return response
                
        except grpc.RpcError as e:
            print(f"Error forwarding task to server {selected_server}: {str(e)}")
            with self.lock:
                if selected_server in self.active_servers:
                    self.active_servers.remove(selected_server)
            return replication_pb2.TaskResponse(status=f"Failed to process task: {str(e)}")

    def monitor_loads(self):
        while True:
            with self.lock:
                print("\nCurrent server loads:")
                for server in self.servers:
                    server_addr = f"{server[0]}:{server[1]}"
                    status = "ACTIVE" if server_addr in self.active_servers else "INACTIVE"
                    load = self.server_metrics[server_addr]["load"]
                    print(f"{server_addr}: {load} tasks ({status})")

                # Fairness: Check for imbalances and redistribute tasks if needed
                active_server_loads = {
                    server: self.server_metrics[server]["load"]
                    for server in self.active_servers
                }
                if active_server_loads:
                    max_load = max(active_server_loads.values())
                    min_load = min(active_server_loads.values())

                    # If imbalance exceeds a threshold, redistribute tasks
                    if max_load - min_load > 2:  # Example threshold
                        print("Imbalance detected. Redistributing tasks...")
                        # Logic to redistribute tasks (placeholder for now)

            time.sleep(5)

    def write(self, data):
        with self.lock:
            available_servers = list(self.active_servers)
            if len(available_servers) < self.W:
                return replication_pb2.WriteResponse(status="Failed: Not enough servers for quorum")

            # Select W servers for the write operation
            selected_servers = random.sample(available_servers, self.W)
            success_count = 0

            for server in selected_servers:
                try:
                    with grpc.insecure_channel(server) as channel:
                        stub = replication_pb2_grpc.ReplicationStub(channel)
                        request = replication_pb2.WriteRequest(data=data)
                        stub.Write(request)
                        success_count += 1
                except grpc.RpcError as e:
                    print(f"Write failed on server {server}: {str(e)}")

            if success_count >= self.W:
                return replication_pb2.WriteResponse(status="Success")
            else:
                return replication_pb2.WriteResponse(status="Failed: Quorum not met")

    def read(self, key):
        with self.lock:
            available_servers = list(self.active_servers)
            if len(available_servers) < self.R:
                return replication_pb2.ReadResponse(status="Failed: Not enough servers for quorum")

            # Select R servers for the read operation
            selected_servers = random.sample(available_servers, self.R)
            responses = []

            for server in selected_servers:
                try:
                    with grpc.insecure_channel(server) as channel:
                        stub = replication_pb2_grpc.ReplicationStub(channel)
                        request = replication_pb2.ReadRequest(key=key)
                        response = stub.Read(request)
                        responses.append(response)
                except grpc.RpcError as e:
                    print(f"Read failed on server {server}: {str(e)}")

            if len(responses) >= self.R:
                # Reconcile responses (e.g., using timestamps or version numbers)
                reconciled_data = self.reconcile_responses(responses)
                return replication_pb2.ReadResponse(status="Success", data=reconciled_data)
            else:
                return replication_pb2.ReadResponse(status="Failed: Quorum not met")

    def reconcile_responses(self, responses):
        # Example reconciliation logic: return the most recent data based on timestamp
        responses.sort(key=lambda r: r.timestamp, reverse=True)
        return responses[0].data

    def Write(self, request, context):
        print(f"Received Write request: key={request.key}, data={request.data}")
        with self.lock:
            available_servers = list(self.active_servers)
            if len(available_servers) < self.W:
                return replication_pb2.WriteResponse(status="Failed: Not enough servers for quorum")

            # Select W servers for the write operation
            selected_servers = random.sample(available_servers, self.W)
            success_count = 0

            for server in selected_servers:
                try:
                    with grpc.insecure_channel(server) as channel:
                        stub = replication_pb2_grpc.ReplicationStub(channel)
                        response = stub.Write(request)
                        if response.status == "Success":
                            success_count += 1
                except grpc.RpcError as e:
                    print(f"Write failed on server {server}: {str(e)}")

            if success_count >= self.W:
                return replication_pb2.WriteResponse(status="Success")
            else:
                return replication_pb2.WriteResponse(status="Failed: Quorum not met")

    def Read(self, request, context):
        print(f"Received Read request: key={request.key}")
        with self.lock:
            available_servers = list(self.active_servers)
            if len(available_servers) < self.R:
                return replication_pb2.ReadResponse(status="Failed: Not enough servers for quorum")

            # Select R servers for the read operation
            selected_servers = random.sample(available_servers, self.R)
            responses = []

            for server in selected_servers:
                try:
                    with grpc.insecure_channel(server) as channel:
                        stub = replication_pb2_grpc.ReplicationStub(channel)
                        response = stub.Read(request)
                        responses.append(response)
                except grpc.RpcError as e:
                    print(f"Read failed on server {server}: {str(e)}")

            if len(responses) >= self.R:
                # Reconcile responses (e.g., using timestamps or version numbers)
                reconciled_data = self.reconcile_responses(responses)
                return replication_pb2.ReadResponse(status="Success", data=reconciled_data)
            else:
                return replication_pb2.ReadResponse(status="Failed: Quorum not met")

    def ReportLoad(self, request, context):
        server_id = request.server_id
        load = request.load

        with self.lock:
            if server_id in self.server_loads:
                self.server_loads[server_id] = load
                print(f"Updated load for server {server_id}: {load}")
                return replication_pb2.LoadResponse(status="Success")
            else:
                print(f"Received load report from unknown server {server_id}")
                return replication_pb2.LoadResponse(status="Failed: Unknown server")

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    servicer = ReplicationServicer()
    replication_pb2_grpc.add_ReplicationServicer_to_server(servicer, server)
    
    # Bind coordinator to all interfaces
    server.add_insecure_port('0.0.0.0:50055')
    
    # Start monitoring threads
    monitor_thread = threading.Thread(target=servicer.monitor_loads, daemon=True)
    availability_thread = threading.Thread(target=servicer.check_server_availability, daemon=True)
    
    monitor_thread.start()
    availability_thread.start()
    
    server.start()
    print("Coordinator started on port 50055")
    server.wait_for_termination()

if __name__ == "__main__":
    serve()