package cs324.bootstrap;

import cs324.proto.*;
import io.grpc.stub.StreamObserver;

import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.*;

public final class BootstrapServiceImpl extends BootstrapServiceGrpc.BootstrapServiceImplBase {
    public static final long WORKER_TIMEOUT_MS = 5_000L;

    private static final class WorkerRecord {
        volatile WorkerInfo info;
        volatile long lastSeen;

        WorkerRecord(WorkerInfo info) {
            this.info = info;
            this.lastSeen = System.currentTimeMillis();
        }
    }

    private final Map<Integer, WorkerRecord> workers = new ConcurrentHashMap<>();
    private final ScheduledExecutorService monitor = Executors.newSingleThreadScheduledExecutor();
    private volatile int coordinatorId;
    private volatile long coordinatorTerm;

    public BootstrapServiceImpl() {
        monitor.scheduleAtFixedRate(this::removeExpiredWorkers, 1, 1, TimeUnit.SECONDS);
    }

    @Override
    public synchronized void registerWorker(RegisterWorkerRequest request,
                                            StreamObserver<RegisterWorkerResponse> out) {
        WorkerInfo worker = request.getWorker();
        WorkerRecord old = workers.get(worker.getId());
        if (isActive(old)) {
            reply(out, RegisterWorkerResponse.newBuilder()
                    .setAccepted(false).setMessage("Worker ID " + worker.getId() + " is already active").build());
            return;
        }

        WorkerRecord record = new WorkerRecord(worker);
        record.info = worker.toBuilder().setLastSeenEpochMs(record.lastSeen).build();
        workers.put(worker.getId(), record);
        System.out.printf("[Bootstrap] Worker %d registered at %s:%d (JAC=%d)%n",
                worker.getId(), worker.getHost(), worker.getPort(), worker.getJac());
        reply(out, RegisterWorkerResponse.newBuilder().setAccepted(true).setMessage("Registered").build());
    }

    @Override
    public synchronized void unregisterWorker(WorkerId request, StreamObserver<Ack> out) {
        int id = request.getWorkerId();
        if (workers.remove(id) != null) System.out.printf("[Bootstrap] Worker %d unregistered%n", id);
        if (coordinatorId == id) coordinatorId = 0;
        reply(out, ack(true, "Unregistered"));
    }

    @Override
    public void heartbeat(WorkerHeartbeat request, StreamObserver<Ack> out) {
        WorkerRecord record = workers.get(request.getWorkerId());
        if (record == null) {
            reply(out, ack(false, "Worker is not registered"));
            return;
        }
        record.lastSeen = System.currentTimeMillis();
        record.info = record.info.toBuilder()
                .setJac(request.getJac())
                .setLastSeenEpochMs(record.lastSeen)
                .build();
        reply(out, ack(true, "Heartbeat accepted"));
    }

    @Override
    public void getActiveWorkers(Empty request, StreamObserver<WorkerList> out) {
        WorkerList.Builder list = WorkerList.newBuilder();
        workers.values().stream()
                .filter(this::isActive)
                .map(r -> r.info.toBuilder().setLastSeenEpochMs(r.lastSeen).build())
                .sorted(Comparator.comparingInt(WorkerInfo::getId))
                .forEach(list::addWorkers);
        reply(out, list.build());
    }

    @Override
    public synchronized void getCoordinator(Empty request, StreamObserver<CoordinatorInfo> out) {
        WorkerRecord record = workers.get(coordinatorId);
        boolean available = isActive(record);
        if (!available) coordinatorId = 0;

        CoordinatorInfo.Builder info = CoordinatorInfo.newBuilder()
                .setAvailable(available)
                .setWorkerId(available ? record.info.getId() : 0)
                .setTerm(coordinatorTerm);
        if (available) info.setHost(record.info.getHost()).setPort(record.info.getPort());
        reply(out, info.build());
    }

    @Override
    public synchronized void setCoordinator(SetCoordinatorRequest request, StreamObserver<Ack> out) {
        WorkerRecord candidate = workers.get(request.getWorkerId());
        if (!isActive(candidate)) {
            reply(out, ack(false, "Coordinator candidate is not active"));
            return;
        }
        if (request.getTerm() < coordinatorTerm) {
            reply(out, ack(false, "Stale election term"));
            return;
        }
        if (request.getTerm() == coordinatorTerm && coordinatorId != 0 && coordinatorId != request.getWorkerId()) {
            reply(out, ack(false, "A coordinator is already committed for this term"));
            return;
        }

        coordinatorId = request.getWorkerId();
        coordinatorTerm = request.getTerm();
        System.out.printf("[Bootstrap] Coordinator = Worker %d, term %d%n", coordinatorId, coordinatorTerm);
        reply(out, ack(true, "Coordinator committed"));
    }

    private synchronized void removeExpiredWorkers() {
        for (Map.Entry<Integer, WorkerRecord> entry : workers.entrySet()) {
            if (isActive(entry.getValue()) || !workers.remove(entry.getKey(), entry.getValue())) continue;
            int id = entry.getKey();
            System.out.printf("[Bootstrap] Worker %d timed out and is marked failed%n", id);
            if (coordinatorId == id) {
                coordinatorId = 0;
                System.out.printf("[Bootstrap] Coordinator Worker %d failed; election required%n", id);
            }
        }
    }

    private boolean isActive(WorkerRecord record) {
        return record != null && System.currentTimeMillis() - record.lastSeen <= WORKER_TIMEOUT_MS;
    }

    private static Ack ack(boolean ok, String message) {
        return Ack.newBuilder().setOk(ok).setMessage(message).build();
    }

    private static <T> void reply(StreamObserver<T> out, T value) {
        out.onNext(value);
        out.onCompleted();
    }

    public void shutdown() {
        monitor.shutdownNow();
    }
}
