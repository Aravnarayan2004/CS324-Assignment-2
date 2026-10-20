# CS324 Assignment 2 - Distributed System

This project is a distributed Java system that uses **gRPC**, **Protocol Buffers**, and **multiple Worker processes**.

The main GUI lets you:

- Start the Bootstrap server
- Start multiple Workers
- Submit `MAX`, `PRIMESUM`, and `PRIMECOUNT` jobs
- View job progress
- View worker/task progress
- Stop or kill Workers
- Test Coordinator failure and recovery

---

## 1. Requirements

Install:

- **Java JDK 17 or newer**
- **Apache Maven**

Check that both are installed:

```bash
java -version
mvn -version
```

If both commands show version information, you are ready.

---

## 2. Open the Project

Open a terminal in the project folder.

The folder should contain:

```text
pom.xml
src/
README.md
```

Example:

```bash
cd CS324-A2
```

If the folder path contains spaces, use quotation marks.

Example:

```bash
cd "/cygdrive/c/Users/Your Name/Downloads/CS324-A2"
```

---

## 3. Build the Project

Run:

```bash
mvn clean compile
```

Wait for:

```text
BUILD SUCCESS
```

Maven will automatically download the required gRPC and Protocol Buffer libraries.

---

## 4. Start the Main GUI

Run:

```bash
mvn exec:java -Dexec.mainClass=cs324.client.ClientGUI
```

The Client GUI will open.

The GUI contains two main tabs:

- **Jobs**
- **System Control**

---

## 5. Start the Distributed System

Open the **System Control** tab.

Use these default settings:

```text
Bootstrap Host: 127.0.0.1
Bootstrap Port: 50050
Number of Workers: 4
```

Click:

```text
Start Everything
```

This starts:

```text
Bootstrap  -> Port 50050
Worker 1   -> Port 50051
Worker 2   -> Port 50052
Worker 3   -> Port 50053
Worker 4   -> Port 50054
```

Wait a few seconds for the Workers to register and for a Coordinator to be elected.

---

## 6. Submit a Job

Open the **Jobs** tab.

### MAX Example

Select:

```text
MAX
```

Enter:

```text
10, 20, 5, 100, 50
```

Expected result:

```text
100
```

### PRIMECOUNT Example

Select:

```text
PRIMECOUNT
```

Enter:

```text
2, 3, 4, 5, 6, 7, 8, 11
```

Expected result:

```text
5
```

### PRIMESUM Example

Select:

```text
PRIMESUM
```

Enter:

```text
Start: 1
End: 10
```

Expected result:

```text
17
```

---

## 7. View Job Progress

For a longer test, use:

```text
PRIMESUM
Start: 1
End: 100000000
```

The GUI will show:

- Job ID
- Job status
- Progress percentage
- Worker/task progress
- Final result

Select the job to view the worker breakdown.

---

## 8. Test Worker Failure

While a long job is running:

1. Open **System Control**.
2. Select a Worker that is **not** the Coordinator.
3. Click **Kill Worker**.

Expected behaviour:

- The Worker stops.
- The system detects the failure.
- Unfinished work is reassigned.
- The job continues.

---

## 9. Test Coordinator Failure

While a long job is running:

1. Open **System Control**.
2. Click **Kill Coordinator**.

Expected behaviour:

- The current Coordinator stops.
- The remaining Workers elect a new Coordinator.
- The new Coordinator restores the job state.
- The job continues automatically.
- The Client GUI reconnects to the new Coordinator.

---

## 10. Add a New Worker

To add another Worker while the system is running:

1. Open **System Control**.
2. Enter a new Worker ID.
3. Click **Start Worker**.

Example:

```text
Worker ID: 5
Port: 50055
```

The new Worker will register with Bootstrap automatically.

---

## 11. Stop the System

When finished:

1. Open **System Control**.
2. Click **Stop Everything**.
3. Close the Client GUI.

---

# Manual Start (Optional)

The GUI is the easiest way to run the system.

You can also start each process manually.

### Bootstrap

```bash
mvn exec:java -Dexec.mainClass=cs324.bootstrap.BootstrapNode -Dexec.args="50050"
```

### Worker 1

```bash
mvn exec:java -Dexec.mainClass=cs324.worker.WorkerNode -Dexec.args="1 50051 127.0.0.1 50050 127.0.0.1"
```

### Worker 2

```bash
mvn exec:java -Dexec.mainClass=cs324.worker.WorkerNode -Dexec.args="2 50052 127.0.0.1 50050 127.0.0.1"
```

### Worker 3

```bash
mvn exec:java -Dexec.mainClass=cs324.worker.WorkerNode -Dexec.args="3 50053 127.0.0.1 50050 127.0.0.1"
```

### Worker 4

```bash
mvn exec:java -Dexec.mainClass=cs324.worker.WorkerNode -Dexec.args="4 50054 127.0.0.1 50050 127.0.0.1"
```

### Client GUI

```bash
mvn exec:java -Dexec.mainClass=cs324.client.ClientGUI
```

---

# Quick Start

For normal use, only these steps are needed:

```text
1. Open the project folder.
2. Run: mvn clean compile
3. Run: mvn exec:java -Dexec.mainClass=cs324.client.ClientGUI
4. Open System Control.
5. Click Start Everything.
6. Open Jobs.
7. Submit a job.
8. Click Stop Everything when finished.
```

---

# Troubleshooting

### Maven is not found

Run:

```bash
mvn -version
```

If Maven is not found, install Maven and add it to your system `PATH`.

### Port already in use

Close old Bootstrap or Worker processes and try again.

### No Coordinator appears

Make sure:

- Bootstrap is running
- At least one Worker is running
- Workers successfully registered
- The Bootstrap host and port are correct

Wait a few seconds for the election to finish.

### Job does not start

Make sure:

- A Coordinator has been elected
- At least one Worker is active
- Bootstrap is still running

---

# Default Ports

| Process | Port |
|---|---:|
| Bootstrap | 50050 |
| Worker 1 | 50051 |
| Worker 2 | 50052 |
| Worker 3 | 50053 |
| Worker 4 | 50054 |

---

## Main Files

```text
pom.xml
src/main/proto/distributed.proto
src/main/java/cs324/client/ClientGUI.java
src/main/java/cs324/bootstrap/BootstrapNode.java
src/main/java/cs324/bootstrap/BootstrapServiceImpl.java
src/main/java/cs324/worker/WorkerNode.java
src/main/java/cs324/worker/WorkerRuntime.java
src/main/java/cs324/worker/WorkerGrpcService.java
src/main/java/cs324/worker/CoordinatorGrpcService.java
```

