package cs324.model;

import cs324.proto.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public final class JobRecord {
    private final String jobId;
    private final JobSpec spec;
    private final String clientRequestId;
    private final Map<String, TaskRecord> tasks = new ConcurrentHashMap<>();
    private volatile JobStatus status;
    private volatile long result;
    private volatile String message;

    public JobRecord(String jobId, JobSpec spec, List<TaskRecord> taskList, String requestId) {
        this(jobId, spec, taskList.isEmpty() ? JobStatus.JOB_COMPLETED : JobStatus.JOB_QUEUED,
                0, taskList.isEmpty() ? "Completed" : "Queued", taskList, requestId);
    }

    private JobRecord(String jobId, JobSpec spec, JobStatus status, long result,
                      String message, List<TaskRecord> taskList, String requestId) {
        this.jobId = jobId;
        this.spec = spec;
        this.status = status;
        this.result = result;
        this.message = message == null ? "" : message;
        this.clientRequestId = requestId == null ? "" : requestId;
        taskList.forEach(t -> tasks.put(t.taskId(), t));
    }

    public String jobId() { return jobId; }
    public JobSpec spec() { return spec; }
    public String clientRequestId() { return clientRequestId; }
    public JobStatus status() { return status; }
    public long result() { return result; }
    public String message() { return message; }

    public void markRunning() {
        if (status == JobStatus.JOB_QUEUED) {
            status = JobStatus.JOB_RUNNING;
            message = "Running";
        }
    }

    public void fail(String reason) {
        status = JobStatus.JOB_FAILED;
        message = reason == null ? "Failed" : reason;
    }

    public boolean terminal() {
        return status == JobStatus.JOB_COMPLETED || status == JobStatus.JOB_FAILED;
    }

    public List<TaskRecord> tasks() {
        return tasks.values().stream().sorted(Comparator.comparing(TaskRecord::taskId)).toList();
    }

    public boolean allTasksComplete() {
        return tasks.values().stream().allMatch(t -> t.status() == TaskStatus.TASK_COMPLETED);
    }

    public synchronized void completeFromTasks() {
        if (!allTasksComplete()) return;
        result = spec.getType() == JobType.JOB_TYPE_MAX
                ? tasks.values().stream().mapToLong(TaskRecord::partialResult).max().orElse(Long.MIN_VALUE)
                : tasks.values().stream().mapToLong(TaskRecord::partialResult).sum();
        status = JobStatus.JOB_COMPLETED;
        message = "Completed";
    }

    public long totalUnits() {
        return tasks.values().stream().mapToLong(TaskRecord::totalUnits).sum();
    }

    public long completedUnits() {
        return tasks.values().stream().mapToLong(t ->
                t.status() == TaskStatus.TASK_COMPLETED
                        ? t.totalUnits()
                        : Math.min(t.completedUnits(), t.totalUnits())).sum();
    }

    public JobState toProto() {
        JobState.Builder out = JobState.newBuilder()
                .setJobId(jobId).setSpec(spec).setStatus(status).setResult(result)
                .setMessage(message).setClientRequestId(clientRequestId);
        tasks().forEach(t -> out.addTasks(t.toProto()));
        return out.build();
    }

    public static JobRecord fromProto(JobState state) {
        List<TaskRecord> taskList = state.getTasksList().stream().map(TaskRecord::fromProto).toList();
        return new JobRecord(state.getJobId(), state.getSpec(), state.getStatus(), state.getResult(),
                state.getMessage(), taskList, state.getClientRequestId());
    }

    public void prepareForCoordinatorRecovery() {
        if (terminal()) return;
        status = JobStatus.JOB_RUNNING;
        message = "Recovered by replacement coordinator";
        tasks.values().forEach(TaskRecord::resetForCoordinatorRecovery);
    }

    public JobProgress toProgress(int coordinatorId, long term) {
        long total = totalUnits(), done = completedUnits();
        int percent = status == JobStatus.JOB_COMPLETED ? 100
                : total == 0 ? 0 : (int) Math.min(100, done * 100L / total);

        JobProgress.Builder out = JobProgress.newBuilder()
                .setJobId(jobId).setStatus(status).setOverallPercent(percent)
                .setCompletedUnits(done).setTotalUnits(total).setResult(result).setMessage(message)
                .setCoordinatorId(coordinatorId).setCoordinatorTerm(term);

        for (TaskRecord task : tasks()) {
            out.addWorkers(WorkerProgress.newBuilder()
                    .setWorkerId(task.workerId()).setTaskId(task.taskId()).setStatus(task.status())
                    .setCompletedUnits(task.completedUnits()).setTotalUnits(task.totalUnits())
                    .setAttempt(task.attempt()).setMessage(task.message()).build());
        }
        return out.build();
    }
}
