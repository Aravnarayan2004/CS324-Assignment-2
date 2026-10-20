package cs324.model;

import cs324.proto.*;

public final class TaskRecord {
    private final TaskSpec baseSpec;
    private TaskStatus status;
    private int workerId, attempt;
    private long completedUnits, partialResult;
    private String message;

    public TaskRecord(TaskSpec spec) {
        this(spec.toBuilder().setAttempt(0).build(), TaskStatus.TASK_PENDING, 0, 0,
                initialPartial(spec), 0, "Pending");
    }

    
    private TaskRecord(TaskSpec spec, TaskStatus status, int workerId, long completedUnits,
                       long partialResult, int attempt, String message) {
        this.baseSpec = spec.toBuilder().setAttempt(0).build();
        this.status = status;
        this.workerId = workerId;
        this.completedUnits = completedUnits;
        this.partialResult = partialResult;
        this.attempt = attempt;
        this.message = message == null ? "" : message;
    }

    private static long initialPartial(TaskSpec spec) {
        return spec.getType() == JobType.JOB_TYPE_MAX ? Long.MIN_VALUE : 0L;
    }

    public synchronized TaskSpec assignTo(int workerId) {
        this.workerId = workerId;
        attempt++;
        resetProgress(TaskStatus.TASK_RUNNING, "Running on Worker " + workerId);
        return currentSpec();
    }

    public synchronized boolean apply(TaskUpdate update) {
        if (update.getAttempt() != attempt || update.getWorkerId() != workerId
                || !update.getTaskId().equals(baseSpec.getTaskId())) return false;
        completedUnits = Math.max(0, Math.min(update.getCompletedUnits(), totalUnits()));
        partialResult = update.getPartialResult();
        status = update.getStatus();
        message = update.getMessage();
        return true;
    }

    public synchronized boolean resetPendingIfAttempt(int expectedAttempt, String reason) {
        if (attempt != expectedAttempt || status == TaskStatus.TASK_COMPLETED) return false;
        resetProgress(TaskStatus.TASK_PENDING, reason == null ? "Requeued" : reason);
        return true;
    }

    public synchronized void resetForCoordinatorRecovery() {
        if (status == TaskStatus.TASK_RUNNING)
            resetProgress(TaskStatus.TASK_PENDING, "Requeued after coordinator recovery");
    }

    private void resetProgress(TaskStatus newStatus, String newMessage) {
        status = newStatus;
        completedUnits = 0;
        partialResult = initialPartial(baseSpec);
        message = newMessage;
    }

    public synchronized TaskSpec currentSpec() {
        return baseSpec.toBuilder().setAttempt(attempt).build();
    }

    public synchronized TaskState toProto() {
        return TaskState.newBuilder()
                .setSpec(currentSpec()).setStatus(status).setWorkerId(workerId)
                .setCompletedUnits(completedUnits).setPartialResult(partialResult)
                .setMessage(message).build();
    }

    public static TaskRecord fromProto(TaskState state) {
        return new TaskRecord(state.getSpec(), state.getStatus(), state.getWorkerId(),
                state.getCompletedUnits(), state.getPartialResult(),
                state.getSpec().getAttempt(), state.getMessage());
    }

    public synchronized String taskId() { return baseSpec.getTaskId(); }
    public synchronized TaskStatus status() { return status; }
    public synchronized int workerId() { return workerId; }
    public synchronized int attempt() { return attempt; }
    public synchronized long completedUnits() { return completedUnits; }
    public synchronized long totalUnits() { return baseSpec.getTotalUnits(); }
    public synchronized long partialResult() { return partialResult; }
    public synchronized String message() { return message; }
}
