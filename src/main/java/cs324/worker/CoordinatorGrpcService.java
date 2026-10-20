package cs324.worker;

import cs324.proto.*;
import io.grpc.Status;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;


public final class CoordinatorGrpcService extends CoordinatorServiceGrpc.CoordinatorServiceImplBase {
    private final WorkerRuntime runtime;

    public CoordinatorGrpcService(WorkerRuntime runtime) {
        this.runtime = runtime;
    }

    @Override
    public void submitJob(SubmitJobRequest request, StreamObserver<SubmitJobReply> out) {
        try {
            reply(out, runtime.submitJob(request));
        } catch (RuntimeException e) {
            out.onError(e);
        }
    }

    @Override
    public void watchJob(JobWatchRequest request, StreamObserver<JobProgress> out) {
        if (!requireCoordinator(out)) return;
        if (out instanceof ServerCallStreamObserver<?> raw) {
            @SuppressWarnings("unchecked")
            ServerCallStreamObserver<JobProgress> server = (ServerCallStreamObserver<JobProgress>) raw;
            server.setOnCancelHandler(() -> runtime.removeWatcher(request.getJobId(), out));
        }
        try {
            runtime.addWatcher(request.getJobId(), out);
        } catch (RuntimeException e) {
            out.onError(e);
        }
    }

    @Override
    public void getJobStatus(JobStatusRequest request, StreamObserver<JobProgress> out) {
        if (!requireCoordinator(out)) return;
        try {
            reply(out, runtime.progressFor(request.getJobId()));
        } catch (RuntimeException e) {
            out.onError(e);
        }
    }

    private boolean requireCoordinator(StreamObserver<?> out) {
        if (runtime.isCoordinator()) return true;
        out.onError(Status.FAILED_PRECONDITION.withDescription("This worker is not the coordinator").asRuntimeException());
        return false;
    }

    private static <T> void reply(StreamObserver<T> out, T value) {
        out.onNext(value);
        out.onCompleted();
    }
}
