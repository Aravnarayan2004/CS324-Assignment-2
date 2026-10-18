package cs324.worker;

import cs324.common.GrpcUtil;
import cs324.model.JobRecord;
import cs324.model.TaskRecord;
import cs324.proto.*;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.Consumer;

public final class WorkerRuntime {
    public final String leaderman = "cs324"; // retained from A1
    private static final long HEARTBEAT_MS = 1_000;
    private static final long MEMBERSHIP_MS = 1_500;
    private static final long COORDINATOR_CHECK_MS = 1_500;
    private static final long ELECTION_WAIT_MS = 1_200;
    private static final long SCHEDULER_TICK_MS = 250;
    private static final int MAX_NEIGHBOURS = 3;

    private final int id, port, bootstrapPort;
    private final String host, bootstrapHost;
    private final ManagedChannel bootstrapChannel;
    private final BootstrapServiceGrpc.BootstrapServiceBlockingStub bootstrap;

    private final AtomicInteger jac = new AtomicInteger();
    private final AtomicInteger coordinatorId = new AtomicInteger();
    private final AtomicLong coordinatorTerm = new AtomicLong();
    private final AtomicBoolean coordinator = new AtomicBoolean();
    private final AtomicBoolean running = new AtomicBoolean();
    private final AtomicBoolean rotationRequested = new AtomicBoolean();
    private final AtomicInteger jobsInTerm = new AtomicInteger();
    private final AtomicInteger coordinatorMisses = new AtomicInteger();
    private final AtomicLong highestElectionTerm = new AtomicLong();

    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(8);
    private final ExecutorService rpcPool = Executors.newCachedThreadPool();
    private final ExecutorService computationPool = Executors.newFixedThreadPool(
            Math.max(2, Runtime.getRuntime().availableProcessors()));

    private final Map<Integer, WorkerInfo> activeWorkers = new ConcurrentHashMap<>();
    private final Map<Integer, WorkerInfo> neighbours = new ConcurrentHashMap<>();
    private final Map<Integer, Long> workerBackoff = new ConcurrentHashMap<>();
    private final Map<Integer, String> busyWorkers = new ConcurrentHashMap<>();
    private final Set<String> seenElections = ConcurrentHashMap.newKeySet();
    private final Set<String> scheduledElections = ConcurrentHashMap.newKeySet();
    private final Set<String> seenAnnouncements = ConcurrentHashMap.newKeySet();

    private final Map<String, JobRecord> jobs = new ConcurrentHashMap<>();
    private final Map<String, String> requestToJob = new ConcurrentHashMap<>();
    private final Map<String, CopyOnWriteArrayList<StreamObserver<JobProgress>>> watchers = new ConcurrentHashMap<>();

    private final AtomicLong stateVersion = new AtomicLong();
    private final AtomicLong replicaVersion = new AtomicLong();
    private final AtomicBoolean replicationQueued = new AtomicBoolean();
    private volatile StateSnapshot latestReplica = StateSnapshot.getDefaultInstance();

    public WorkerRuntime(int id, String host, int port, String bootstrapHost, int bootstrapPort) {
        this.id = id;
        this.host = host;
        this.port = port;
        this.bootstrapHost = bootstrapHost;
        this.bootstrapPort = bootstrapPort;
        bootstrapChannel = GrpcUtil.channel(bootstrapHost, bootstrapPort);
        bootstrap = BootstrapServiceGrpc.newBlockingStub(bootstrapChannel);
    }

    public int id() { return id; }
    public int coordinatorId() { return coordinatorId.get(); }
    public long coordinatorTerm() { return coordinatorTerm.get(); }
    public boolean isCoordinator() { return coordinator.get(); }
    public boolean rotationRequested() { return rotationRequested.get(); }
    public ExecutorService computationPool() { return computationPool; }

    public WorkerInfo selfInfo() {
        return WorkerInfo.newBuilder().setId(id).setHost(host).setPort(port).setJac(jac.get())
                .setLastSeenEpochMs(System.currentTimeMillis()).build();
    }

    public void start() {
        RegisterWorkerResponse registration = bootstrap.withDeadlineAfter(2, TimeUnit.SECONDS)
                .registerWorker(RegisterWorkerRequest.newBuilder().setWorker(selfInfo()).build());
        if (!registration.getAccepted()) throw new IllegalStateException(registration.getMessage());

        running.set(true);
        refreshMembership();
        every("heartbeat", this::heartbeat, 0, HEARTBEAT_MS);
        every("membership refresh", this::refreshMembership, 0, MEMBERSHIP_MS);
        every("coordinator monitor", this::monitorCoordinator, 500, COORDINATOR_CHECK_MS);
        every("coordinator scheduler", this::coordinatorTick, 500, SCHEDULER_TICK_MS);
        every("state replication", this::periodicReplication, 1_000, 1_000);
        System.out.printf("[Worker %d] Registered with Bootstrap %s:%d%n", id, bootstrapHost, bootstrapPort);
    }

    private void every(String name, Runnable action, long delay, long period) {
        scheduler.scheduleAtFixedRate(() -> safe(name, action), delay, period, TimeUnit.MILLISECONDS);
    }

    private void safe(String name, Runnable action) {
        if (!running.get()) return;
        try {
            action.run();
        } catch (Exception e) {
            System.err.printf("[Worker %d] %s failed: %s%n", id, name, shortMessage(e));
        }
    }

    private void heartbeat() {
        Ack ack = bootstrap.withDeadlineAfter(800, TimeUnit.MILLISECONDS).heartbeat(WorkerHeartbeat.newBuilder()
                .setWorkerId(id).setJac(jac.get()).setKnownCoordinatorTerm(coordinatorTerm.get()).build());
        if (!ack.getOk()) System.err.printf("[Worker %d] Bootstrap rejected heartbeat: %s%n", id, ack.getMessage());
    }

    private void refreshMembership() {
        WorkerList list = bootstrap.withDeadlineAfter(1, TimeUnit.SECONDS)
                .getActiveWorkers(Empty.getDefaultInstance());
        Set<Integer> seen = new HashSet<>();
        for (WorkerInfo worker : list.getWorkersList()) {
            activeWorkers.put(worker.getId(), worker);
            seen.add(worker.getId());
        }
        activeWorkers.keySet().removeIf(workerId -> !seen.contains(workerId));
        rebuildNeighbours();
    }

    private void rebuildNeighbours() {
        List<WorkerInfo> all = activeWorkers.values().stream()
                .sorted(Comparator.comparingInt(WorkerInfo::getId)).toList();
        Map<Integer, WorkerInfo> next = new HashMap<>();

        int self = -1;
        for (int i = 0; i < all.size(); i++) if (all.get(i).getId() == id) { self = i; break; }
        if (self >= 0 && all.size() > 1) {
            WorkerInfo successor = all.get((self + 1) % all.size());
            if (successor.getId() != id) next.put(successor.getId(), successor);
        }

        List<WorkerInfo> random = new ArrayList<>(all);
        random.removeIf(w -> w.getId() == id || next.containsKey(w.getId()));
        Collections.shuffle(random);
        for (WorkerInfo worker : random) {
            if (next.size() >= MAX_NEIGHBOURS) break;
            next.put(worker.getId(), worker);
        }
        neighbours.clear();
        neighbours.putAll(next);
    }

    private void monitorCoordinator() {
        CoordinatorInfo info = bootstrap.withDeadlineAfter(900, TimeUnit.MILLISECONDS)
                .getCoordinator(Empty.getDefaultInstance());
        coordinatorTerm.accumulateAndGet(info.getTerm(), Math::max);

        if (!info.getAvailable()) {
            coordinatorMisses.incrementAndGet();
            startElection("No active coordinator", 0);
            return;
        }

        coordinatorId.set(info.getWorkerId());
        if (info.getWorkerId() == id) {
            coordinatorMisses.set(0);
            if (!coordinator.get()) activateAsCoordinator(info.getTerm(), "Bootstrap selected this worker");
            return;
        }

        if (coordinator.get()) stepDown(info.getWorkerId(), info.getTerm());
        if (pingWorker(info.getHost(), info.getPort())) coordinatorMisses.set(0);
        else if (coordinatorMisses.incrementAndGet() >= 2)
            startElection("Coordinator RPC unreachable", info.getWorkerId());
    }

    private boolean pingWorker(String targetHost, int targetPort) {
        ManagedChannel channel = GrpcUtil.channel(targetHost, targetPort);
        try {
            WorkerServiceGrpc.newBlockingStub(channel).withDeadlineAfter(800, TimeUnit.MILLISECONDS)
                    .ping(PingRequest.newBuilder().setFromWorkerId(id).build());
            return true;
        } catch (Exception e) {
            return false;
        } finally {
            GrpcUtil.shutdown(channel);
        }
    }

    public void startElection(String reason, int excludedWorkerId) {
        long base = coordinatorTerm.get();
        try {
            base = Math.max(base, bootstrap.withDeadlineAfter(700, TimeUnit.MILLISECONDS)
                    .getCoordinator(Empty.getDefaultInstance()).getTerm());
        } catch (Exception ignored) {}

        long term = base + 1;
        if (highestElectionTerm.getAndAccumulate(term, Math::max) >= term) return;

        ElectionMessage election = ElectionMessage.newBuilder()
                .setElectionId(UUID.randomUUID().toString()).setTerm(term)
                .setOriginWorkerId(id).setSenderWorkerId(id).setExcludedWorkerId(excludedWorkerId)
                .setReason(reason == null ? "" : reason).build();
        System.out.printf("[Worker %d] Starting election %s term=%d reason=%s excluded=%d%n",
                id, election.getElectionId(), term, reason, excludedWorkerId);
        onElection(election);
    }

    public void onElection(ElectionMessage election) {
        coordinatorTerm.accumulateAndGet(election.getTerm(), Math::max);
        highestElectionTerm.accumulateAndGet(election.getTerm(), Math::max);
        if (!seenElections.add(election.getElectionId())) return;

        for (WorkerInfo worker : neighbours.values()) {
            if (worker.getId() != election.getSenderWorkerId())
                send(worker, stub -> stub.election(election.toBuilder().setSenderWorkerId(id).build()));
        }
        if (scheduledElections.add(election.getElectionId()))
            scheduler.schedule(() -> finishElection(election), ELECTION_WAIT_MS, TimeUnit.MILLISECONDS);
    }

    private void finishElection(ElectionMessage election) {
        if (!running.get()) return;
        try {
            refreshMembership();
            WorkerInfo winner = activeWorkers.values().stream()
                    .filter(w -> w.getId() != election.getExcludedWorkerId())
                    .min((a, b) -> {
                        int byJac = Integer.compare(a.getJac(), b.getJac());
                        return byJac != 0 ? byJac : Integer.compare(b.getId(), a.getId());
                    }).orElse(null);
            if (winner != null && winner.getId() == id) commitCoordinator(election);
        } catch (Exception e) {
            System.err.printf("[Worker %d] Election finalisation failed: %s%n", id, shortMessage(e));
        }
    }

    private void commitCoordinator(ElectionMessage election) {
        try {
            Ack ack = bootstrap.withDeadlineAfter(1, TimeUnit.SECONDS).setCoordinator(SetCoordinatorRequest.newBuilder()
                    .setWorkerId(id).setTerm(election.getTerm()).build());
            if (!ack.getOk()) {
                System.out.printf("[Worker %d] Election term %d rejected: %s%n",
                        id, election.getTerm(), ack.getMessage());
                return;
            }

            activateAsCoordinator(election.getTerm(), "Won election " + election.getElectionId());
            onCoordinatorAnnouncement(CoordinatorAnnouncement.newBuilder()
                    .setElectionId(election.getElectionId()).setTerm(election.getTerm())
                    .setCoordinator(selfInfo()).setSenderWorkerId(id).build());
        } catch (Exception e) {
            System.err.printf("[Worker %d] Could not commit coordinator: %s%n", id, shortMessage(e));
        }
    }

    public void onCoordinatorAnnouncement(CoordinatorAnnouncement announcement) {
        String key = announcement.getElectionId() + ':' + announcement.getTerm() + ':'
                + announcement.getCoordinator().getId();
        if (!seenAnnouncements.add(key) || announcement.getTerm() < coordinatorTerm.get()) return;

        coordinatorTerm.set(announcement.getTerm());
        coordinatorId.set(announcement.getCoordinator().getId());
        if (coordinatorId.get() == id) {
            if (!coordinator.get()) activateAsCoordinator(announcement.getTerm(), "Received coordinator announcement");
        } else if (coordinator.get()) stepDown(coordinatorId.get(), announcement.getTerm());

        for (WorkerInfo worker : neighbours.values()) {
            if (worker.getId() != announcement.getSenderWorkerId())
                send(worker, stub -> stub.announceCoordinator(
                        announcement.toBuilder().setSenderWorkerId(id).build()));
        }
    }

    private synchronized void activateAsCoordinator(long term, String reason) {
        coordinatorTerm.accumulateAndGet(term, Math::max);
        coordinatorId.set(id);
        coordinator.set(true);
        rotationRequested.set(false);
        jobsInTerm.set(0);
        coordinatorMisses.set(0);
        busyWorkers.clear();

        if (latestReplica.getVersion() > stateVersion.get()) loadReplica(latestReplica);
        else jobs.values().forEach(JobRecord::prepareForCoordinatorRecovery);

        System.out.printf("[Worker %d] *** COORDINATOR term=%d (%s), recovered jobs=%d ***%n",
                id, coordinatorTerm.get(), reason, jobs.size());
        replicateStateNow();
    }

    private void stepDown(int newCoordinatorId, long term) {
        coordinator.set(false);
        coordinatorId.set(newCoordinatorId);
        coordinatorTerm.accumulateAndGet(term, Math::max);
        rotationRequested.set(false);
        busyWorkers.clear();
        closeWatchersForFailover();
        System.out.printf("[Worker %d] Stepped down; coordinator=Worker %d term=%d%n",
                id, newCoordinatorId, term);
    }

    private void closeWatchersForFailover() {
        RuntimeException error = Status.UNAVAILABLE
                .withDescription("Coordinator changed; reconnect to the replacement coordinator").asRuntimeException();
        watchers.values().forEach(list -> {
            list.forEach(observer -> { try { observer.onError(error); } catch (Exception ignored) {} });
            list.clear();
        });
    }

    public SubmitJobReply submitJob(SubmitJobRequest request) {
        if (!coordinator.get()) throw Status.FAILED_PRECONDITION
                .withDescription("Worker " + id + " is not the coordinator").asRuntimeException();
        if (rotationRequested.get()) throw Status.UNAVAILABLE
                .withDescription("Coordinator rotation is in progress").asRuntimeException();

        validateJob(request.getJob());
        String requestId = request.getClientRequestId();
        if (!requestId.isBlank()) {
            String existing = requestToJob.get(requestId);
            if (existing != null) return acceptedReply(existing, "Duplicate request returned existing job");
        }

        String jobId = "JOB-" + UUID.randomUUID();
        List<TaskRecord> tasks = splitIntoTasks(jobId, request.getJob(), Math.max(1, activeWorkers.size()));
        JobRecord job = new JobRecord(jobId, request.getJob(), tasks, requestId);
        jobs.put(jobId, job);
        if (!requestId.isBlank()) requestToJob.put(requestId, jobId);

        int jobNumber = jobsInTerm.incrementAndGet();
        int newJac = jac.incrementAndGet();
        System.out.printf("[Coordinator %d] Accepted %s (%s), term job #%d, JAC=%d, tasks=%d%n",
                id, jobId, request.getJob().getType(), jobNumber, newJac, tasks.size());
        safe("heartbeat", this::heartbeat);
        publishProgress(job);
        replicateStateNow();
        return acceptedReply(jobId, "Accepted");
    }

    private SubmitJobReply acceptedReply(String jobId, String message) {
        return SubmitJobReply.newBuilder().setAccepted(true).setJobId(jobId).setMessage(message)
                .setCoordinatorId(id).setCoordinatorTerm(coordinatorTerm.get()).build();
    }

    private void validateJob(JobSpec spec) {
        switch (spec.getType()) {
            case JOB_TYPE_MAX -> {
                if (spec.getNumbersCount() == 0) invalid("MAX requires at least one number");
            }
            case JOB_TYPE_PRIMESUM -> {
                if (spec.getStart() > spec.getEnd()) invalid("PRIMESUM start must be <= end");
            }
            case JOB_TYPE_PRIMECOUNT -> { }
            default -> invalid("Unknown job type");
        }
    }

    private static void invalid(String message) {
        throw Status.INVALID_ARGUMENT.withDescription(message).asRuntimeException();
    }

    private List<TaskRecord> splitIntoTasks(String jobId, JobSpec spec, int workerCount) {
        List<TaskRecord> tasks = new ArrayList<>();
        long units = spec.getType() == JobType.JOB_TYPE_PRIMESUM
                ? (long) spec.getEnd() - spec.getStart() + 1 : spec.getNumbersCount();
        if (units <= 0) return tasks;

        int chunks = (int) Math.min(units, Math.max(8, workerCount * 4L));
        long chunkSize = Math.max(1, (units + chunks - 1) / chunks);
        int taskNo = 0;

        if (spec.getType() == JobType.JOB_TYPE_PRIMESUM) {
            for (long start = spec.getStart(); start <= spec.getEnd();) {
                long end = Math.min(spec.getEnd(), start + chunkSize - 1);
                tasks.add(new TaskRecord(TaskSpec.newBuilder()
                        .setJobId(jobId).setTaskId(jobId + "-T" + taskNo++).setType(spec.getType())
                        .setStart((int) start).setEnd((int) end).setTotalUnits(end - start + 1).build()));
                start = end + 1;
            }
        } else {
            for (int start = 0; start < spec.getNumbersCount();) {
                int end = (int) Math.min(spec.getNumbersCount(), start + chunkSize);
                TaskSpec.Builder task = TaskSpec.newBuilder()
                        .setJobId(jobId).setTaskId(jobId + "-T" + taskNo++).setType(spec.getType())
                        .setTotalUnits(end - start);
                for (int i = start; i < end; i++) task.addNumbers(spec.getNumbers(i));
                tasks.add(new TaskRecord(task.build()));
                start = end;
            }
        }
        return tasks;
    }

    private void coordinatorTick() {
        if (!coordinator.get() || rotationRequested.get()) return;
        long now = System.currentTimeMillis();
        Set<Integer> activeIds = new HashSet<>(activeWorkers.keySet());

        for (JobRecord job : jobs.values()) {
            if (job.terminal()) continue;
            job.markRunning();

            for (TaskRecord task : job.tasks()) {
                if (task.status() == TaskStatus.TASK_RUNNING && !activeIds.contains(task.workerId())) {
                    int failedWorker = task.workerId(), attempt = task.attempt();
                    if (task.resetPendingIfAttempt(attempt, "Worker " + failedWorker + " failed; task requeued")) {
                        busyWorkers.remove(failedWorker, task.taskId() + "#" + attempt);
                        System.out.printf("[Coordinator %d] Requeued %s after Worker %d failure%n",
                                id, task.taskId(), failedWorker);
                        changed(job);
                    }
                }
            }

            Iterator<WorkerInfo> available = activeWorkers.values().stream()
                    .filter(w -> workerBackoff.getOrDefault(w.getId(), 0L) <= now)
                    .filter(w -> !busyWorkers.containsKey(w.getId()))
                    .sorted(Comparator.comparingInt(WorkerInfo::getId)).iterator();

            for (TaskRecord task : job.tasks()) {
                if (task.status() != TaskStatus.TASK_PENDING || !available.hasNext()) continue;
                WorkerInfo worker = available.next();
                TaskSpec assigned = task.assignTo(worker.getId());
                String token = task.taskId() + "#" + assigned.getAttempt();
                busyWorkers.put(worker.getId(), token);
                changed(job);
                dispatchTask(job, task, assigned, worker, coordinatorTerm.get(), token);
            }

            if (job.allTasksComplete() && !job.terminal()) {
                job.completeFromTasks();
                System.out.printf("[Coordinator %d] %s complete, result=%d%n", id, job.jobId(), job.result());
                publishProgress(job);
                replicateStateNow();
            }
        }
        maybeRotateCoordinator();
    }

    private void dispatchTask(JobRecord job, TaskRecord task, TaskSpec assigned,
                              WorkerInfo worker, long term, String busyToken) {
        rpcPool.submit(() -> {
            ManagedChannel channel = GrpcUtil.channel(worker.getHost(), worker.getPort());
            try {
                Iterator<TaskUpdate> updates = WorkerServiceGrpc.newBlockingStub(channel).executeTask(assigned);
                while (updates.hasNext()) {
                    if (!sameCoordinatorTerm(term)) return;
                    if (task.apply(updates.next())) changed(job);
                }
                if (sameCoordinatorTerm(term) && task.status() != TaskStatus.TASK_COMPLETED)
                    requeue(job, task, assigned, worker, term, false, "Task stream ended early; task requeued");
            } catch (StatusRuntimeException e) {
                requeue(job, task, assigned, worker, term, true,
                        "RPC to Worker " + worker.getId() + " failed; task requeued");
            } catch (Exception e) {
                requeue(job, task, assigned, worker, term, true, "Worker communication lost; task requeued");
            } finally {
                busyWorkers.remove(worker.getId(), busyToken);
                GrpcUtil.shutdown(channel);
            }
        });
    }

    private void requeue(JobRecord job, TaskRecord task, TaskSpec assigned,
                         WorkerInfo worker, long term, boolean backoff, String reason) {
        if (!sameCoordinatorTerm(term)) return;
        if (backoff) workerBackoff.put(worker.getId(), System.currentTimeMillis() + 3_000);
        if (task.resetPendingIfAttempt(assigned.getAttempt(), reason)) {
            System.out.printf("[Coordinator %d] Requeued %s from Worker %d%n", id, task.taskId(), worker.getId());
            changed(job);
        }
    }

    private boolean sameCoordinatorTerm(long term) {
        return coordinator.get() && coordinatorTerm.get() == term;
    }

    private void changed(JobRecord job) {
        publishProgress(job);
        requestReplication();
    }

    private void maybeRotateCoordinator() {
        if (jobsInTerm.get() < 5 || rotationRequested.get()
                || jobs.values().stream().anyMatch(j -> !j.terminal())) return;
        if (rotationRequested.compareAndSet(false, true)) {
            System.out.printf("[Coordinator %d] Five jobs completed; starting rotation election%n", id);
            replicateStateNow();
            scheduler.schedule(() -> startElection("Five coordinator jobs completed", 0), 300, TimeUnit.MILLISECONDS);
        }
    }

    public JobRecord getJob(String jobId) { return jobs.get(jobId); }

    public JobProgress progressFor(String jobId) {
        JobRecord job = jobs.get(jobId);
        if (job == null) throw Status.NOT_FOUND.withDescription("Unknown job " + jobId).asRuntimeException();
        return job.toProgress(id, coordinatorTerm.get());
    }

    public void addWatcher(String jobId, StreamObserver<JobProgress> observer) {
        JobRecord job = jobs.get(jobId);
        if (job == null) throw Status.NOT_FOUND.withDescription("Unknown job " + jobId).asRuntimeException();
        CopyOnWriteArrayList<StreamObserver<JobProgress>> list =
                watchers.computeIfAbsent(jobId, ignored -> new CopyOnWriteArrayList<>());
        list.add(observer);
        observer.onNext(job.toProgress(id, coordinatorTerm.get()));
        if (job.terminal()) {
            list.remove(observer);
            observer.onCompleted();
        }
    }

    public void removeWatcher(String jobId, StreamObserver<JobProgress> observer) {
        List<StreamObserver<JobProgress>> list = watchers.get(jobId);
        if (list != null) list.remove(observer);
    }

    private void publishProgress(JobRecord job) {
        CopyOnWriteArrayList<StreamObserver<JobProgress>> list = watchers.get(job.jobId());
        if (list == null || list.isEmpty()) return;
        JobProgress progress = job.toProgress(id, coordinatorTerm.get());
        for (StreamObserver<JobProgress> observer : list) {
            try {
                observer.onNext(progress);
                if (job.terminal()) {
                    observer.onCompleted();
                    list.remove(observer);
                }
            } catch (Exception e) {
                list.remove(observer);
            }
        }
    }

    public synchronized void acceptReplica(StateSnapshot snapshot) {
        if (snapshot.getVersion() < replicaVersion.get()) return;
        replicaVersion.set(snapshot.getVersion());
        latestReplica = snapshot;
    }

    private void loadReplica(StateSnapshot snapshot) {
        jobs.clear();
        requestToJob.clear();
        for (JobState state : snapshot.getJobsList()) {
            JobRecord job = JobRecord.fromProto(state);
            job.prepareForCoordinatorRecovery();
            jobs.put(job.jobId(), job);
            if (!job.clientRequestId().isBlank()) requestToJob.put(job.clientRequestId(), job.jobId());
        }
        stateVersion.set(snapshot.getVersion());
        busyWorkers.clear();
        System.out.printf("[Worker %d] Loaded replicated state version %d (%d jobs)%n",
                id, snapshot.getVersion(), jobs.size());
    }

    private StateSnapshot buildSnapshot() {
        StateSnapshot.Builder out = StateSnapshot.newBuilder()
                .setVersion(stateVersion.incrementAndGet()).setCoordinatorTerm(coordinatorTerm.get())
                .setCoordinatorId(id).setCreatedEpochMs(System.currentTimeMillis());
        jobs.values().stream().sorted(Comparator.comparing(JobRecord::jobId))
                .forEach(job -> out.addJobs(job.toProto()));
        return out.build();
    }

    private void requestReplication() {
        if (!coordinator.get() || !replicationQueued.compareAndSet(false, true)) return;
        scheduler.schedule(() -> {
            replicationQueued.set(false);
            if (coordinator.get()) replicateStateNow();
        }, 150, TimeUnit.MILLISECONDS);
    }

    private void periodicReplication() {
        if (coordinator.get()) replicateStateNow();
    }

    private void replicateStateNow() {
        if (!coordinator.get()) return;
        StateSnapshot snapshot = buildSnapshot();
        latestReplica = snapshot;
        replicaVersion.set(snapshot.getVersion());
        for (WorkerInfo worker : activeWorkers.values())
            if (worker.getId() != id) send(worker, stub -> stub.replicateState(snapshot));
    }

    private void send(WorkerInfo worker, Consumer<WorkerServiceGrpc.WorkerServiceBlockingStub> action) {
        rpcPool.submit(() -> {
            ManagedChannel channel = GrpcUtil.channel(worker.getHost(), worker.getPort());
            try {
                action.accept(WorkerServiceGrpc.newBlockingStub(channel).withDeadlineAfter(1, TimeUnit.SECONDS));
            } catch (Exception ignored) {
            } finally {
                GrpcUtil.shutdown(channel);
            }
        });
    }

    public void computeTask(TaskSpec task, StreamObserver<TaskUpdate> observer, AtomicBoolean cancelled) {
        computationPool.submit(() -> {
            long completed = 0;
            long partial = task.getType() == JobType.JOB_TYPE_MAX ? Long.MIN_VALUE : 0;
            long reportEvery = Math.max(1, task.getTotalUnits() / 100);
            long lastReport = 0;
            try {
                observer.onNext(taskUpdate(task, TaskStatus.TASK_RUNNING, 0, partial, "Started"));
                if (task.getType() == JobType.JOB_TYPE_PRIMESUM) {
                    for (long value = task.getStart(); value <= task.getEnd(); value++) {
                        if (cancelled.get() || Thread.currentThread().isInterrupted()) return;
                        if (isPrime((int) value)) partial += value;
                        completed++;
                        long now = System.nanoTime();
                        if (completed % reportEvery == 0 && now - lastReport >= 50_000_000L) {
                            observer.onNext(taskUpdate(task, TaskStatus.TASK_RUNNING, completed, partial, "Running"));
                            lastReport = now;
                        }
                    }
                } else {
                    for (int number : task.getNumbersList()) {
                        if (cancelled.get() || Thread.currentThread().isInterrupted()) return;
                        if (task.getType() == JobType.JOB_TYPE_MAX) partial = Math.max(partial, number);
                        else if (isPrime(number)) partial++;
                        completed++;
                        long now = System.nanoTime();
                        if (completed % reportEvery == 0 && now - lastReport >= 50_000_000L) {
                            observer.onNext(taskUpdate(task, TaskStatus.TASK_RUNNING, completed, partial, "Running"));
                            lastReport = now;
                        }
                    }
                }
                if (!cancelled.get()) {
                    observer.onNext(taskUpdate(task, TaskStatus.TASK_COMPLETED, completed, partial, "Completed"));
                    observer.onCompleted();
                }
            } catch (Exception e) {
                if (!cancelled.get()) observer.onError(e);
            }
        });
    }

    private TaskUpdate taskUpdate(TaskSpec task, TaskStatus status, long completed, long partial, String message) {
        return TaskUpdate.newBuilder().setJobId(task.getJobId()).setTaskId(task.getTaskId())
                .setWorkerId(id).setStatus(status).setCompletedUnits(completed)
                .setTotalUnits(task.getTotalUnits()).setPartialResult(partial)
                .setAttempt(task.getAttempt()).setMessage(message).build();
    }

    private static boolean isPrime(int n) {
        if (n < 2) return false;
        if (n == 2) return true;
        if ((n & 1) == 0) return false;
        for (int i = 3; i <= n / i; i += 2) if (n % i == 0) return false;
        return true;
    }

    public PingReply pingReply() {
        return PingReply.newBuilder().setOk(true).setWorkerId(id).setCoordinator(coordinator.get())
                .setCoordinatorTerm(coordinatorTerm.get()).build();
    }

    public void shutdown() {
        if (!running.compareAndSet(true, false)) return;
        try {
            bootstrap.withDeadlineAfter(600, TimeUnit.MILLISECONDS)
                    .unregisterWorker(WorkerId.newBuilder().setWorkerId(id).build());
        } catch (Exception ignored) {}
        scheduler.shutdownNow();
        rpcPool.shutdownNow();
        computationPool.shutdownNow();
        GrpcUtil.shutdown(bootstrapChannel);
        System.out.printf("[Worker %d] Stopped%n", id);
    }

    private static String shortMessage(Throwable e) {
        String message = e.getMessage();
        return message == null || message.isBlank() ? e.getClass().getSimpleName() : message;
    }
}
