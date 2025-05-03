# Mini 3 - Replication Algorithm

This project implements a replication (NWR) algorithm using Python and gRPC. The algorithm adapts to network and load pressures and supports weak and strong scaling tests.

## Project Structure
- `server.py`: Implements the server logic.
- `coordinator.py`: Manages load balancing and replication.
- `client.py`: Sends tasks/messages to the network for testing.
- `proto/`: Contains `.proto` files for gRPC communication.

## Setup
1. Install dependencies:
   ```bash
   pip install grpcio grpcio-tools
   ```
2. Generate gRPC code:
   ```bash
   python -m grpc_tools.protoc -I=proto --python_out=. --grpc_python_out=. proto/*.proto
   ```
3. Run the servers and client:
   ```bash
   python server.py
   python coordinator.py
   python client.py
   ```

## Testing
- Validate the algorithm with at least 5 servers across 2 computers.
- Measure performance under different load conditions.

## Notes
- Ensure Python 3.8+ is installed.
- Use multiple terminals to run the servers and client.