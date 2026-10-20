package cs324.worker;

import cs324.proto.*;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;

import java.util.concurrent.atomic.AtomicBoolean;

public final class WorkerGrpcService extends WorkerServiceGrpc.WorkerServiceImplBase {
    private final WorkerRuntime runtime;

    public WorkerGrpcService(WorkerRuntime runtime) {
        this.runtime = runtime;
    }

    
    @Override
    public void ping(PingRequest request, StreamObserver<PingReply> out) {
        reply(out, runtime.pingReply());
    }

    @Override
    public void election(ElectionMessage request, StreamObserver<Ack> out) {
        runtime.onElection(request);
        reply(out, ack("Election message accepted"));
    }

    @Override
    public void announceCoordinator(CoordinatorAnnouncement request, StreamObserver<Ack> out) {
        runtime.onCoordinatorAnnouncement(request);
        reply(out, ack("Coordinator announcement accepted"));
    }

    @Override
    public void executeTask(TaskSpec request, StreamObserver<TaskUpdate> out) {
        AtomicBoolean cancelled = new AtomicBoolean();
        if (out instanceof ServerCallStreamObserver<?> server)
            server.setOnCancelHandler(() -> cancelled.set(true));
        runtime.computeTask(request, out, cancelled);
    }

    @Override
    public void replicateState(StateSnapshot request, StreamObserver<Ack> out) {
        runtime.acceptReplica(request);
        reply(out, ack("State replicated"));
    }

    private static Ack ack(String message) {
        return Ack.newBuilder().setOk(true).setMessage(message).build();
    }

    private static <T> void reply(StreamObserver<T> out, T value) {
        out.onNext(value);
        out.onCompleted();
    }
}
