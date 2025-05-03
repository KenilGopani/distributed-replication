import grpc
import replication_pb2
import replication_pb2_grpc
import time

def send_task():
    # Simulate sending a task to the server
    with grpc.insecure_channel('localhost:50051') as channel:
        print("Task sent to server")

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

if __name__ == "__main__":
    coordinator_address = "localhost:50055"

    # Test Write Operation
    print("Testing Write Operation...")
    test_write(coordinator_address, key="test_key", data="test_data")

    # Test Read Operation
    print("Testing Read Operation...")
    test_read(coordinator_address, key="test_key")

    # Weak Scaling Test
    print("Starting Weak Scaling Test...")
    for num_tasks in [10, 50, 100]:
        print(f"Testing with {num_tasks} tasks...")
        test_weak_scaling(coordinator_address, num_tasks)

    # Strong Scaling Test
    print("Starting Strong Scaling Test...")
    num_tasks = 100  # Fixed number of tasks
    print(f"Testing with {num_tasks} tasks...")
    test_strong_scaling(coordinator_address, num_tasks)