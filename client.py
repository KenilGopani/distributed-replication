import grpc
import replication_pb2
import replication_pb2_grpc
import time
import random

def send_task_to_server(server_address, task_id):
    with grpc.insecure_channel(server_address) as channel:
        stub = replication_pb2_grpc.ReplicationStub(channel)
        request = replication_pb2.TaskRequest(task_id=task_id)
        response = stub.HandleTask(request)
        print(f"Task {task_id} sent to {server_address}. Response: {response.status}")

def test_write(coordinator_address, key, data):
    with grpc.insecure_channel(coordinator_address) as channel:
        stub = replication_pb2_grpc.ReplicationStub(channel)
        request = replication_pb2.WriteRequest(key=key, data=data)
        response = stub.Write(request)
        print(f"Write Response: {response.status}")

def test_read(coordinator_address, key):
    with grpc.insecure_channel(coordinator_address) as channel:
        stub = replication_pb2_grpc.ReplicationStub(channel)
        request = replication_pb2.ReadRequest(key=key)
        response = stub.Read(request)
        print(f"Read Response: {response.status}, Data: {response.data}, Timestamp: {response.timestamp}")

def test_weak_scaling(coordinator_address, num_tasks):
    with grpc.insecure_channel(coordinator_address) as channel:
        stub = replication_pb2_grpc.ReplicationStub(channel)
        start_time = time.time()
        for i in range(num_tasks):
            request = replication_pb2.TaskRequest(task_id=f"task_{i}")
            response = stub.HandleTask(request)
            print(f"Task {i} Response: {response.status}")
        end_time = time.time()
        print(f"Processed {num_tasks} tasks in {end_time - start_time:.2f} seconds")

def test_strong_scaling(coordinator_address, num_tasks):
    with grpc.insecure_channel(coordinator_address) as channel:
        stub = replication_pb2_grpc.ReplicationStub(channel)
        start_time = time.time()
        for i in range(num_tasks):
            request = replication_pb2.TaskRequest(task_id=f"task_{i}")
            response = stub.HandleTask(request)
            print(f"Task {i} Response: {response.status}")
        end_time = time.time()
        print(f"Processed {num_tasks} tasks in {end_time - start_time:.2f} seconds")

def test_write_and_read():
    servers = [
        "127.0.0.1:50051",
        "127.0.0.1:50052",
        "127.0.0.1:50053",
        "127.0.0.1:50054",
        "127.0.0.1:50055"
    ]

    # Test Write Operation: track which server handled each write
    print("Testing Write Operation...")
    write_servers = []
    for i in range(3):
        key = f"key{i}"
        data = f"value{i}"
        server_address = random.choice(servers)
        write_servers.append(server_address)
        test_write(server_address, key, data)

    # Test Read Operation: read from the same servers used for writes
    print("Testing Read Operation...")
    for i in range(3):
        key = f"key{i}"
        server_address = write_servers[i]
        test_read(server_address, key)

def main():
    servers = [
        "10.0.0.108:50051",
        "10.0.0.108:50052",
        "10.0.0.108:50053",
        "10.0.0.223:50057",
        "10.0.0.223:50058"
    ]

    # Simulate sending tasks to random servers
    for i in range(10):
        task_id = f"task_{i}"
        server_address = random.choice(servers)
        send_task_to_server(server_address, task_id)

if __name__ == "__main__":
    # main()
    test_write_and_read()