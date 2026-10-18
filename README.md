# CS324 Distributed Systems 2026 - Assignment 2

This project is a gRPC/Protocol Buffers rewrite and extension of the submitted A1 system. It keeps the A1 ideas of a Bootstrap node, independent Worker JVMs, a worker-elected coordinator, JAC-based leader selection, Java-threaded computation, and coordinator rotation after five jobs, while adding the A2 requirements for progress streaming and failure recovery.

## Requirements

- JDK 17 or later
- Maven 3.9+ (or a recent Maven 3.x)

## Build

From the project directory:

```bash
mvn clean compile
```

The Protocol Buffer and gRPC Java sources are generated automatically from:

```text
src/main/proto/distributed.proto
```

## Run the system

### Recommended: use the integrated Client GUI

After building once with `mvn clean compile`, start the client:

```bash
mvn exec:java -Dexec.mainClass=cs324.client.ClientGUI
```

`ClientGUI.java` now contains both the assignment client and the local process controls in one window. Use the two tabs:

- **Jobs** - submit `MAX`, `PRIMESUM`, and `PRIMECOUNT` jobs and view real-time progress.
- **System Control** - start/stop Bootstrap and workers, view cluster membership and logs, and trigger worker/coordinator failures for testing.

The launcher uses the Bootstrap host/port shown on the **Jobs** tab. With the defaults, Worker 1 uses port `50051`, Worker 2 uses `50052`, and so on. Set the worker count to `4` and click **Start Everything** to launch Bootstrap and Workers 1-4 as **separate JVM processes**.

The System Control tab provides **Start Bootstrap**, **Start Worker**, **Start Workers 1-N**, **Start Everything**, **Kill Worker**, **Kill Coordinator**, **Stop Everything**, a live process log, and a cluster table showing active workers, ports, JAC, and the current coordinator. Selecting a worker in the table copies its ID into the worker selector.

The launcher is only a convenience interface; Bootstrap, workers, coordinator communication, job execution, election, and recovery still use gRPC between separate processes.

### Manual terminal startup

If you want to run each process manually, open separate terminals. Start the Bootstrap node first:

```bash
mvn exec:java -Dexec.mainClass=cs324.bootstrap.BootstrapNode -Dexec.args="50050"
```

Start at least three workers. The arguments are:

```text
<ID> <PORT> [BOOTSTRAP_HOST] [BOOTSTRAP_PORT] [ADVERTISED_HOST]
```

Example:

```bash
mvn exec:java -Dexec.mainClass=cs324.worker.WorkerNode -Dexec.args="1 50051 127.0.0.1 50050 127.0.0.1"
mvn exec:java -Dexec.mainClass=cs324.worker.WorkerNode -Dexec.args="2 50052 127.0.0.1 50050 127.0.0.1"
mvn exec:java -Dexec.mainClass=cs324.worker.WorkerNode -Dexec.args="3 50053 127.0.0.1 50050 127.0.0.1"
mvn exec:java -Dexec.mainClass=cs324.worker.WorkerNode -Dexec.args="4 50054 127.0.0.1 50050 127.0.0.1"
```

Workers automatically register, form an overlay of neighbours, and elect a coordinator. There is no manual ENTER step.

Start the Swing client:

```bash
mvn exec:java -Dexec.mainClass=cs324.client.ClientGUI
```

The client talks to Bootstrap at `127.0.0.1:50050` by default. It discovers the current coordinator automatically.

## Supported jobs

- `MAX(numbers)` - maximum integer from a list
- `PRIMESUM(start, end)` - sum of all primes in the inclusive range
- `PRIMECOUNT(numbers)` - number of primes in a list

For `MAX` and `PRIMECOUNT`, enter comma- or whitespace-separated integers in **Numbers**. For `PRIMESUM`, use **Start** and **End**.

## A2 features implemented

### gRPC and Protocol Buffers

All communication between Bootstrap, workers, coordinator, and clients is gRPC. There is no Java RMI code in A2. `distributed.proto` defines Bootstrap membership, leader-election messages, task execution, coordinator-state replication, job submission, and server-streaming job progress.

### Real-time progress

The coordinator splits each job into multiple task chunks. Workers stream `TaskUpdate` messages containing actual processed work units. The coordinator derives overall percentage from completed work and streams `JobProgress` messages to clients with `WatchJob`.

The Swing table uses a real `JProgressBar` renderer. Click/select a job or its progress cell to see the task/worker breakdown, including worker ID, task, actual units processed, task status, retry attempt, and recovery messages.

### Worker failure tolerance

Each worker sends a Bootstrap heartbeat every second. Bootstrap removes workers that have not been seen for five seconds. The coordinator also reacts immediately to a failed task RPC.

A task on a failed/unreachable worker is returned to `TASK_PENDING` and reassigned. Attempts are numbered, so late updates from an older attempt cannot overwrite the current attempt. A slow worker is not declared failed merely because computation takes time: as long as its process heartbeat and task stream remain alive, the task is allowed to continue.

### Dynamic worker joining

Jobs are split into more chunks than the current worker count. The coordinator refreshes active membership continuously and assigns pending chunks to workers that become available, including newly joined workers.

### Coordinator failure tolerance

Workers periodically discover and ping the current coordinator. If there is no coordinator, or the coordinator cannot be reached repeatedly, a new election starts automatically.

Election messages are flooded through the worker overlay. The leader rule retained from A1 is:

1. lowest JAC wins;
2. if JAC is equal, highest worker ID wins.

Election terms prevent stale coordinator announcements from replacing a newer coordinator.

The coordinator continuously replicates a `StateSnapshot` to the other active workers. The snapshot contains jobs, their input specifications, task assignments/statuses, completed partial results, retry attempts, and final results. After failover, the new coordinator loads the latest local replica, keeps completed tasks, changes previously running tasks back to pending, and reassigns them. This allows a submitted job to continue without restarting the system.

The client also reconnects its progress stream after coordinator failover. It asks Bootstrap for the replacement coordinator and resumes `WatchJob(jobId)`.

### A1 coordinator rotation

The coordinator JAC increases when it accepts a job. After five jobs assigned in a coordinator term have completed, the system automatically starts a new election. The rotation waits for the active jobs to finish, preventing an unnecessary handover in the middle of the normal five-job rotation. Crash recovery can still hand over during a running job.

## Failure demonstrations

### Worker crash during a long job

1. Start Bootstrap and at least three workers.
2. Submit a large `PRIMESUM`, for example `1` to `5000000` or larger depending on the machine.
3. Observe worker/task progress in the GUI.
4. Stop one non-coordinator Worker JVM with Ctrl+C or terminate it.
5. The job continues. The failed worker's unfinished task is requeued and later assigned to another worker.

### Add a worker while a job is running

1. Start Bootstrap with two workers.
2. Submit a long `PRIMESUM` job.
3. Start Worker 3 while the job is still running.
4. After membership refresh, Worker 3 can receive one of the remaining pending chunks.

### Coordinator crash during a job

1. Start at least three workers and submit a long job.
2. Read the coordinator ID from the worker logs or client label.
3. Terminate that coordinator Worker JVM.
4. The remaining workers detect failure, elect a replacement, restore replicated state, and reassign work that was running when the coordinator died.
5. The GUI's stream disconnects briefly, rediscovers the new coordinator, and continues showing the same job ID.

## Important source files

```text
src/main/proto/distributed.proto
src/main/java/cs324/bootstrap/BootstrapNode.java
src/main/java/cs324/bootstrap/BootstrapServiceImpl.java
src/main/java/cs324/worker/WorkerNode.java
src/main/java/cs324/worker/WorkerRuntime.java
src/main/java/cs324/worker/WorkerGrpcService.java
src/main/java/cs324/worker/CoordinatorGrpcService.java
src/main/java/cs324/model/JobRecord.java
src/main/java/cs324/model/TaskRecord.java
src/main/java/cs324/client/ClientGUI.java
```

## Git requirement from the assignment

The assignment requires a new A2 repository whose **initial commit is the submitted A1 code**, plus meaningful commits by all group members. Do not replace that history with one final code dump.

A suitable workflow is:

```bash
git init
# copy the exact submitted A1 files first
git add .
git commit -m "Initial commit: submitted Assignment 1"

# then migrate/add A2 in multiple meaningful commits
git add .
git commit -m "Replace RMI interfaces with gRPC and protobuf"
# continue with separate progress, failure recovery, GUI, testing commits
```

Before submission, append `git log` to the report and include the required contribution/minutes page. Those items depend on the group's real development history and should not be fabricated.
