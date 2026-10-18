package cs324.client;

import cs324.common.GrpcUtil;
import cs324.proto.*;
import io.grpc.ManagedChannel;
import io.grpc.StatusRuntimeException;

import javax.swing.*;
import javax.swing.table.AbstractTableModel;
import javax.swing.table.TableCellRenderer;
import java.awt.*;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public final class ClientGUI extends JFrame {
    private final JTextField bootstrapHostField = new JTextField("127.0.0.1", 12);
    private final JTextField bootstrapPortField = new JTextField("50050", 6);
    private final JComboBox<String> jobTypeBox = new JComboBox<>(new String[]{"MAX", "PRIMESUM", "PRIMECOUNT"});
    private final JTextField startField = new JTextField("1", 10);
    private final JTextField endField = new JTextField("100000", 10);
    private final JTextArea numbersArea = new JTextArea("10, 20, 5, 100, 50, 97, 101", 3, 36);
    private final JButton submitButton = new JButton("Submit Job");
    private final JLabel coordinatorLabel = new JLabel("Coordinator: discovering...");

    private final JobTableModel jobModel = new JobTableModel();
    private final JTable jobTable = new JTable(jobModel);
    private final JTextArea breakdownArea = textArea();
    private final Map<String, JobProgress> progressByJob = new ConcurrentHashMap<>();
    private final ExecutorService clientPool = Executors.newCachedThreadPool();
    private final AtomicBoolean closing = new AtomicBoolean();
    private final JTabbedPane tabs = new JTabbedPane();
    private final SystemControlPanel systemPanel = new SystemControlPanel();
    private volatile String selectedJobId;

    public ClientGUI() {
        this("127.0.0.1", 50050);
    }

    public ClientGUI(String host, int port) {
        super("CS324 Distributed System");
        bootstrapHostField.setText(host == null || host.isBlank() ? "127.0.0.1" : host);
        bootstrapPortField.setText(String.valueOf(port));
        setDefaultCloseOperation(DO_NOTHING_ON_CLOSE);
        setSize(1150, 780);
        setMinimumSize(new Dimension(950, 650));
        setLocationRelativeTo(null);
        tabs.addTab("Jobs", buildJobsPanel());
        tabs.addTab("System Control", systemPanel);
        setContentPane(tabs);
        addWindowListener(new WindowAdapter() {
            @Override public void windowClosing(WindowEvent e) { closeApplication(); }
        });
    }

    private JPanel buildJobsPanel() {
        JPanel root = new JPanel(new BorderLayout(8, 8));
        root.setBorder(BorderFactory.createEmptyBorder(10, 10, 10, 10));

        JPanel input = new JPanel();
        input.setLayout(new BoxLayout(input, BoxLayout.Y_AXIS));
        input.setBorder(BorderFactory.createTitledBorder("Connection and Job Input"));
        input.add(row(new JLabel("Bootstrap:"), bootstrapHostField, new JLabel(":"), bootstrapPortField,
                new JLabel("Job:"), jobTypeBox, new JLabel("Start:"), startField, new JLabel("End:"), endField));
        input.add(row(new JLabel("Numbers:"), new JScrollPane(numbersArea)));
        JButton clear = new JButton("Clear Inputs");
        clear.addActionListener(e -> { numbersArea.setText(""); startField.setText("1"); endField.setText("100000"); });
        input.add(row(submitButton, clear, coordinatorLabel));
        submitButton.addActionListener(e -> submitJob());
        root.add(input, BorderLayout.NORTH);

        jobTable.setRowHeight(28);
        jobTable.setAutoCreateRowSorter(true);
        jobTable.getColumnModel().getColumn(2).setCellRenderer(new ProgressRenderer());
        jobTable.getSelectionModel().addListSelectionListener(e -> {
            if (!e.getValueIsAdjusting()) showSelectedJob();
        });

        JPanel jobs = titled("Submitted Jobs - live gRPC progress", new JScrollPane(jobTable));
        JPanel details = titled("Worker-level Breakdown", new JScrollPane(breakdownArea));
        breakdownArea.setText("Select a job to see task/worker progress.\n");
        JSplitPane split = new JSplitPane(JSplitPane.VERTICAL_SPLIT, jobs, details);
        split.setResizeWeight(0.62);
        root.add(split, BorderLayout.CENTER);
        return root;
    }

    private static JPanel row(Component... components) {
        JPanel panel = new JPanel(new FlowLayout(FlowLayout.LEFT, 6, 4));
        for (Component c : components) panel.add(c);
        return panel;
    }

    private static JPanel titled(String title, Component content) {
        JPanel panel = new JPanel(new BorderLayout());
        panel.setBorder(BorderFactory.createTitledBorder(title));
        panel.add(content);
        return panel;
    }

    private static JTextArea textArea() {
        JTextArea area = new JTextArea();
        area.setEditable(false);
        area.setFont(new Font(Font.MONOSPACED, Font.PLAIN, 12));
        return area;
    }

    private void submitJob() {
        JobSpec spec;
        try {
            spec = buildJobSpec();
        } catch (Exception e) {
            error("Invalid input", e.getMessage());
            return;
        }

        submitButton.setEnabled(false);
        String requestId = UUID.randomUUID().toString();
        clientPool.submit(() -> {
            try {
                SubmitJobReply reply = submitWithRetry(spec, requestId);
                SwingUtilities.invokeLater(() -> jobModel.add(reply.getJobId(), typeName(spec.getType())));
                watchJob(reply.getJobId());
            } catch (Exception e) {
                SwingUtilities.invokeLater(() -> error("Submission error", "Could not submit job: " + shortMessage(e)));
            } finally {
                SwingUtilities.invokeLater(() -> submitButton.setEnabled(true));
            }
        });
    }

    private JobSpec buildJobSpec() {
        String selected = Objects.toString(jobTypeBox.getSelectedItem(), "MAX");
        if (selected.equals("PRIMESUM")) {
            int start = Integer.parseInt(startField.getText().trim());
            int end = Integer.parseInt(endField.getText().trim());
            if (start > end) throw new IllegalArgumentException("Start must be <= End.");
            return JobSpec.newBuilder().setType(JobType.JOB_TYPE_PRIMESUM).setStart(start).setEnd(end).build();
        }

        List<Integer> numbers = parseNumbers(numbersArea.getText());
        if (selected.equals("MAX") && numbers.isEmpty())
            throw new IllegalArgumentException("MAX requires at least one number.");
        JobSpec.Builder spec = JobSpec.newBuilder().setType(
                selected.equals("MAX") ? JobType.JOB_TYPE_MAX : JobType.JOB_TYPE_PRIMECOUNT);
        numbers.forEach(spec::addNumbers);
        return spec.build();
    }

    private static List<Integer> parseNumbers(String text) {
        if (text == null || text.isBlank()) return List.of();
        List<Integer> result = new ArrayList<>();
        for (String part : text.trim().split("[,\\s]+")) result.add(Integer.parseInt(part));
        return result;
    }

    private SubmitJobReply submitWithRetry(JobSpec spec, String requestId) throws Exception {
        Exception last = null;
        for (int i = 0; i < 20 && !closing.get(); i++) {
            ManagedChannel channel = null;
            try {
                CoordinatorInfo coordinator = discoverCoordinator();
                if (!coordinator.getAvailable()) {
                    setCoordinator("Coordinator: election in progress...");
                    sleep(500);
                    continue;
                }
                setCoordinator(coordinatorText(coordinator));
                channel = GrpcUtil.channel(coordinator.getHost(), coordinator.getPort());
                SubmitJobReply reply = CoordinatorServiceGrpc.newBlockingStub(channel)
                        .withDeadlineAfter(3, TimeUnit.SECONDS)
                        .submitJob(SubmitJobRequest.newBuilder().setJob(spec).setClientRequestId(requestId).build());
                if (!reply.getAccepted()) throw new IllegalStateException(reply.getMessage());
                return reply;
            } catch (Exception e) {
                last = e;
                sleep(500);
            } finally {
                GrpcUtil.shutdown(channel);
            }
        }
        throw last == null ? new IllegalStateException("No coordinator became available") : last;
    }

    private CoordinatorInfo discoverCoordinator() {
        ManagedChannel channel = GrpcUtil.channel(bootstrapHost(), bootstrapPort());
        try {
            return BootstrapServiceGrpc.newBlockingStub(channel).withDeadlineAfter(1, TimeUnit.SECONDS)
                    .getCoordinator(Empty.getDefaultInstance());
        } finally {
            GrpcUtil.shutdown(channel);
        }
    }

    private void watchJob(String jobId) {
        while (!closing.get()) {
            ManagedChannel channel = null;
            try {
                CoordinatorInfo coordinator = discoverCoordinator();
                if (!coordinator.getAvailable()) {
                    setCoordinator("Coordinator: recovering...");
                    sleep(500);
                    continue;
                }
                setCoordinator(coordinatorText(coordinator));
                channel = GrpcUtil.channel(coordinator.getHost(), coordinator.getPort());
                Iterator<JobProgress> stream = CoordinatorServiceGrpc.newBlockingStub(channel)
                        .watchJob(JobWatchRequest.newBuilder().setJobId(jobId).build());
                while (stream.hasNext() && !closing.get()) {
                    JobProgress progress = stream.next();
                    onProgress(progress);
                    if (terminal(progress.getStatus())) return;
                }
            } catch (StatusRuntimeException ignored) {
                sleep(500); // normal during failover/rotation
            } catch (Exception ignored) {
                sleep(500);
            } finally {
                GrpcUtil.shutdown(channel);
            }
        }
    }

    private void onProgress(JobProgress progress) {
        progressByJob.put(progress.getJobId(), progress);
        SwingUtilities.invokeLater(() -> {
            jobModel.update(progress);
            if (progress.getJobId().equals(selectedJobId)) renderBreakdown(progress);
        });
    }

    private void showSelectedJob() {
        int row = jobTable.getSelectedRow();
        if (row < 0) return;
        selectedJobId = jobModel.jobIdAt(jobTable.convertRowIndexToModel(row));
        JobProgress progress = progressByJob.get(selectedJobId);
        if (progress == null) breakdownArea.setText("Waiting for progress for " + selectedJobId + "...\n");
        else renderBreakdown(progress);
    }

    private void renderBreakdown(JobProgress p) {
        StringBuilder b = new StringBuilder()
                .append("Job: ").append(p.getJobId()).append('\n')
                .append("Coordinator: Worker ").append(p.getCoordinatorId()).append(" | term ")
                .append(p.getCoordinatorTerm()).append('\n')
                .append("Overall: ").append(p.getOverallPercent()).append("% (")
                .append(p.getCompletedUnits()).append('/').append(p.getTotalUnits()).append(" units)\n")
                .append("Status: ").append(statusName(p.getStatus())).append('\n');
        if (p.getStatus() == JobStatus.JOB_COMPLETED) b.append("Result: ").append(p.getResult()).append('\n');
        b.append(String.format("%n%-8s %-24s %-11s %-15s %-7s %s%n",
                "Worker", "Task", "Status", "Units", "Try", "Message"));
        for (WorkerProgress w : p.getWorkersList()) {
            b.append(String.format("%-8s %-24s %-11s %-15s %-7d %s%n",
                    w.getWorkerId() == 0 ? "pending" : w.getWorkerId(), shortText(w.getTaskId(), 24),
                    statusName(w.getStatus()), w.getCompletedUnits() + "/" + w.getTotalUnits(),
                    w.getAttempt(), w.getMessage()));
        }
        breakdownArea.setText(b.toString());
        breakdownArea.setCaretPosition(0);
    }

    private String bootstrapHost() {
        String value = bootstrapHostField.getText().trim();
        return value.isEmpty() ? "127.0.0.1" : value;
    }

    private int bootstrapPort() {
        try {
            int port = Integer.parseInt(bootstrapPortField.getText().trim());
            if (port < 1 || port > 65535) throw new NumberFormatException();
            return port;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Bootstrap port must be 1-65535.");
        }
    }

    private void setCoordinator(String text) {
        SwingUtilities.invokeLater(() -> coordinatorLabel.setText(text));
    }

    private static String coordinatorText(CoordinatorInfo c) {
        return "Coordinator: Worker " + c.getWorkerId() + " (term " + c.getTerm() + ")";
    }

    private static boolean terminal(JobStatus status) {
        return status == JobStatus.JOB_COMPLETED || status == JobStatus.JOB_FAILED;
    }

    private static String typeName(JobType type) {
        return type.name().replace("JOB_TYPE_", "");
    }

    private static String statusName(Enum<?> status) {
        return status.name().replace("JOB_", "").replace("TASK_", "");
    }

    private static String shortText(String text, int max) {
        return text.length() <= max ? text : text.substring(0, max - 3) + "...";
    }

    private static void sleep(long ms) {
        try { Thread.sleep(ms); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }

    private void error(String title, String message) {
        JOptionPane.showMessageDialog(this, message, title, JOptionPane.ERROR_MESSAGE);
    }

    private static String shortMessage(Throwable e) {
        String m = e.getMessage();
        return m == null || m.isBlank() ? e.getClass().getSimpleName() : m.replace('\n', ' ');
    }

    private void closeApplication() {
        boolean stop = false;
        int running = systemPanel.runningProcesses();
        if (running > 0) {
            int answer = JOptionPane.showConfirmDialog(this,
                    "There are " + running + " locally started process(es). Stop them before closing?",
                    "Close", JOptionPane.YES_NO_CANCEL_OPTION);
            if (answer == JOptionPane.CANCEL_OPTION || answer == JOptionPane.CLOSED_OPTION) return;
            stop = answer == JOptionPane.YES_OPTION;
        }
        closing.set(true);
        clientPool.shutdownNow();
        systemPanel.shutdown(stop);
        dispose();
    }

    public static void main(String[] args) {
        String host = args.length > 0 ? args[0] : "127.0.0.1";
        int port = args.length > 1 ? Integer.parseInt(args[1]) : 50050;
        SwingUtilities.invokeLater(() -> new ClientGUI(host, port).setVisible(true));
    }

    private final class SystemControlPanel extends JPanel {
        private static final DateTimeFormatter TIME = DateTimeFormatter.ofPattern("HH:mm:ss");
        private final JSpinner workerId = new JSpinner(new SpinnerNumberModel(1, 1, 999, 1));
        private final JSpinner workerCount = new JSpinner(new SpinnerNumberModel(4, 1, 32, 1));
        private final JTextArea log = textArea();
        private final ClusterTableModel clusterModel = new ClusterTableModel();
        private final JTable clusterTable = new JTable(clusterModel);
        private final JLabel summary = new JLabel("Cluster: checking...");
        private final Map<Integer, ManagedProcess> workerProcesses = new ConcurrentHashMap<>();
        private final ExecutorService processPool = Executors.newCachedThreadPool();
        private final ScheduledExecutorService monitor = Executors.newSingleThreadScheduledExecutor();
        private final AtomicBoolean stopped = new AtomicBoolean();
        private volatile ManagedProcess bootstrapProcess;
        private volatile ClusterSnapshot cluster = ClusterSnapshot.offline();

        SystemControlPanel() {
            setLayout(new BorderLayout(8, 8));
            setBorder(BorderFactory.createEmptyBorder(10, 10, 10, 10));

            JButton startBootstrap = button("Start Bootstrap", () -> async(this::startBootstrap));
            JButton startWorker = button("Start Worker", () -> async(() -> startWorker(workerId())));
            JButton startWorkers = button("Start Workers 1-N", () -> async(this::startWorkers));
            JButton startAll = button("Start Everything", () -> async(this::startEverything));
            JButton killWorker = button("Kill Worker", () -> async(() -> stopWorker(workerId(), true)));
            JButton killCoordinator = button("Kill Coordinator", () -> async(this::killCoordinator));
            JButton stopAll = button("Stop Everything", () -> async(() -> stopEverything(false)));
            JButton jobs = button("Go to Jobs", () -> tabs.setSelectedIndex(0));
            JButton clear = button("Clear Log", () -> log.setText(""));

            JPanel controls = new JPanel();
            controls.setLayout(new BoxLayout(controls, BoxLayout.Y_AXIS));
            controls.setBorder(BorderFactory.createTitledBorder("Local Cluster Control"));
            controls.add(row(new JLabel("Uses Bootstrap host/port from Jobs tab."), summary));
            controls.add(row(new JLabel("Worker ID:"), workerId, startWorker, killWorker,
                    new JLabel("Worker count:"), workerCount, startWorkers));
            controls.add(row(startBootstrap, startAll, killCoordinator, stopAll, jobs));
            add(controls, BorderLayout.NORTH);

            clusterTable.setRowHeight(24);
            clusterTable.setAutoCreateRowSorter(true);
            clusterTable.getSelectionModel().addListSelectionListener(e -> {
                if (e.getValueIsAdjusting() || clusterTable.getSelectedRow() < 0) return;
                ClusterRow r = clusterModel.rowAt(clusterTable.convertRowIndexToModel(clusterTable.getSelectedRow()));
                if (r.workerId() != null) workerId.setValue(r.workerId());
            });

            JPanel logPanel = titled("Live Process Log", new JScrollPane(log));
            logPanel.add(row(clear), BorderLayout.NORTH);
            JSplitPane split = new JSplitPane(JSplitPane.VERTICAL_SPLIT,
                    titled("Cluster Status", new JScrollPane(clusterTable)), logPanel);
            split.setResizeWeight(0.42);
            add(split, BorderLayout.CENTER);

            monitor.scheduleWithFixedDelay(() -> {
                if (stopped.get()) return;
                ClusterSnapshot snapshot = queryCluster();
                cluster = snapshot;
                SwingUtilities.invokeLater(() -> showCluster(snapshot));
            }, 0, 1, TimeUnit.SECONDS);
        }

        private JButton button(String text, Runnable action) {
            JButton button = new JButton(text);
            button.addActionListener(e -> action.run());
            return button;
        }

        private int workerId() { return ((Number) workerId.getValue()).intValue(); }
        private int workerCount() { return ((Number) workerCount.getValue()).intValue(); }
        private int workerPort(int id) { return bootstrapPort() + id; }

        private void startBootstrap() {
            if (alive(bootstrapProcess) || cluster.bootstrapReachable()) {
                writeLog("Launcher", "Bootstrap is already running/reachable.");
                return;
            }
            bootstrapProcess = launch("Bootstrap", "cs324.bootstrap.BootstrapNode", String.valueOf(bootstrapPort()));
        }

        private void startWorker(int id) {
            if (alive(workerProcesses.get(id)) || cluster.workers().stream().anyMatch(w -> w.getId() == id)) {
                writeLog("Launcher", "Worker " + id + " is already active.");
                return;
            }
            int port = workerPort(id);
            String args = id + " " + port + " " + bootstrapHost() + " " + bootstrapPort() + " 127.0.0.1";
            ManagedProcess p = launch("Worker " + id, "cs324.worker.WorkerNode", args);
            if (p != null) workerProcesses.put(id, p);
        }

        private void startWorkers() {
            for (int id = 1; id <= workerCount() && !stopped.get(); id++) {
                startWorker(id);
                sleep(350);
            }
        }

        private void startEverything() {
            startBootstrap();
            sleep(900);
            startWorkers();
            sleep(500);
            SwingUtilities.invokeLater(() -> tabs.setSelectedIndex(0));
        }

        private void killCoordinator() {
            if (!cluster.coordinator().getAvailable()) {
                writeLog("Launcher", "No coordinator is currently reported.");
                return;
            }
            int id = cluster.coordinator().getWorkerId();
            if (!alive(workerProcesses.get(id))) {
                writeLog("Launcher", "Coordinator Worker " + id + " was not started by this GUI.");
                return;
            }
            stopWorker(id, true);
        }

        private void stopWorker(int id, boolean force) {
            ManagedProcess p = workerProcesses.remove(id);
            if (!alive(p)) {
                writeLog("Launcher", "Worker " + id + " is not running locally.");
                return;
            }
            writeLog("Launcher", (force ? "Killing " : "Stopping ") + "Worker " + id + "...");
            terminate(p.process(), force);
        }

        private void stopEverything(boolean force) {
            new ArrayList<>(workerProcesses.keySet()).forEach(id -> stopWorker(id, force));
            if (alive(bootstrapProcess)) terminate(bootstrapProcess.process(), force);
            bootstrapProcess = null;
        }

        private ManagedProcess launch(String label, String mainClass, String args) {
            try {
                ProcessBuilder pb = new ProcessBuilder(mavenCommand(mainClass, args));
                pb.directory(new File(System.getProperty("user.dir"))).redirectErrorStream(true);
                Process process = pb.start();
                ManagedProcess managed = new ManagedProcess(label, process);
                processPool.submit(() -> readOutput(managed));
                process.onExit().thenRun(() -> writeLog("Launcher", label + " exited with code " + process.exitValue()));
                writeLog("Launcher", "Started " + label + " (PID " + process.pid() + ").");
                return managed;
            } catch (IOException e) {
                writeLog("Launcher", "Could not start " + label + ": " + e.getMessage());
                SwingUtilities.invokeLater(() -> error("Launcher error",
                        "Could not start " + label + ". Make sure Maven is on PATH."));
                return null;
            }
        }

        private List<String> mavenCommand(String mainClass, String args) {
            String main = "-Dexec.mainClass=" + mainClass;
            String execArgs = "-Dexec.args=" + args;
            if (isWindows()) return List.of("cmd.exe", "/c", "mvn -q exec:java " + main + " \"" + execArgs + "\"");
            return List.of("mvn", "-q", "exec:java", main, execArgs);
        }

        private void readOutput(ManagedProcess process) {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(
                    process.process().getInputStream(), StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) writeLog(process.label(), line);
            } catch (IOException ignored) {}
        }

        private ClusterSnapshot queryCluster() {
            ManagedChannel channel = null;
            try {
                channel = GrpcUtil.channel(bootstrapHost(), bootstrapPort());
                var stub = BootstrapServiceGrpc.newBlockingStub(channel).withDeadlineAfter(700, TimeUnit.MILLISECONDS);
                WorkerList workers = stub.getActiveWorkers(Empty.getDefaultInstance());
                return new ClusterSnapshot(true, workers.getWorkersList(),
                        stub.getCoordinator(Empty.getDefaultInstance()), "");
            } catch (Exception e) {
                return new ClusterSnapshot(false, List.of(), CoordinatorInfo.getDefaultInstance(), shortMessage(e));
            } finally {
                GrpcUtil.shutdown(channel);
            }
        }

        private void showCluster(ClusterSnapshot snapshot) {
            String leader = snapshot.coordinator().getAvailable()
                    ? "Worker " + snapshot.coordinator().getWorkerId() + " (term " + snapshot.coordinator().getTerm() + ")"
                    : "none";
            summary.setText(snapshot.bootstrapReachable()
                    ? "Bootstrap online | Workers: " + snapshot.workers().size() + " | Coordinator: " + leader
                    : "Bootstrap unreachable");
            clusterModel.setRows(clusterRows(snapshot));
        }

        private List<ClusterRow> clusterRows(ClusterSnapshot snapshot) {
            Map<Integer, WorkerInfo> active = new HashMap<>();
            snapshot.workers().forEach(w -> active.put(w.getId(), w));
            Set<Integer> ids = new TreeSet<>(active.keySet());
            ids.addAll(workerProcesses.keySet());
            List<ClusterRow> rows = new ArrayList<>();
            rows.add(new ClusterRow("Bootstrap", null, bootstrapPort(), alive(bootstrapProcess) ? "RUNNING" : "-",
                    snapshot.bootstrapReachable() ? "ONLINE" : "OFFLINE", "Membership", null));
            for (int id : ids) {
                WorkerInfo w = active.get(id);
                boolean leader = w != null && snapshot.coordinator().getAvailable()
                        && snapshot.coordinator().getWorkerId() == id;
                rows.add(new ClusterRow("Worker", id, w == null ? workerPort(id) : w.getPort(),
                        alive(workerProcesses.get(id)) ? "RUNNING" : "-",
                        w == null ? "INACTIVE" : "ACTIVE", leader ? "COORDINATOR" : "Worker",
                        w == null ? null : w.getJac()));
            }
            return rows;
        }

        private void terminate(Process process, boolean force) {
            if (process == null || !process.isAlive()) return;
            try {
                if (isWindows()) {
                    List<String> cmd = new ArrayList<>(List.of("taskkill", "/PID", String.valueOf(process.pid()), "/T"));
                    if (force) cmd.add("/F");
                    new ProcessBuilder(cmd).start().waitFor(3, TimeUnit.SECONDS);
                } else {
                    process.toHandle().descendants().forEach(h -> { if (force) h.destroyForcibly(); else h.destroy(); });
                    if (force) process.destroyForcibly(); else process.destroy();
                }
            } catch (Exception e) {
                process.destroyForcibly();
            }
        }

        private void async(Runnable action) {
            processPool.submit(() -> {
                try { action.run(); }
                catch (Exception e) { writeLog("Launcher", "Operation failed: " + shortMessage(e)); }
            });
        }

        private void writeLog(String source, String message) {
            String line = "[" + LocalTime.now().format(TIME) + "] [" + source + "] " + message + "\n";
            SwingUtilities.invokeLater(() -> {
                log.append(line);
                if (log.getDocument().getLength() > 200_000) log.setText(log.getText().substring(50_000));
                log.setCaretPosition(log.getDocument().getLength());
            });
        }

        int runningProcesses() {
            return (alive(bootstrapProcess) ? 1 : 0)
                    + (int) workerProcesses.values().stream().filter(SystemControlPanel::alive).count();
        }

        void shutdown(boolean stopProcesses) {
            stopped.set(true);
            if (stopProcesses) stopEverything(false);
            monitor.shutdownNow();
            processPool.shutdownNow();
        }

        private static boolean alive(ManagedProcess p) { return p != null && p.process().isAlive(); }
        private static boolean isWindows() { return System.getProperty("os.name", "").toLowerCase().contains("win"); }

        private record ManagedProcess(String label, Process process) {}
        private record ClusterSnapshot(boolean bootstrapReachable, List<WorkerInfo> workers,
                                       CoordinatorInfo coordinator, String error) {
            static ClusterSnapshot offline() {
                return new ClusterSnapshot(false, List.of(), CoordinatorInfo.getDefaultInstance(), "");
            }
        }
        private record ClusterRow(String component, Integer workerId, int port, String local,
                                  String cluster, String role, Integer jac) {}

        private final class ClusterTableModel extends AbstractTableModel {
            private final String[] columns = {"Component", "ID", "Port", "Local", "Cluster", "Role", "JAC"};
            private List<ClusterRow> rows = List.of();
            void setRows(List<ClusterRow> rows) { this.rows = rows; fireTableDataChanged(); }
            ClusterRow rowAt(int row) { return rows.get(row); }
            @Override public int getRowCount() { return rows.size(); }
            @Override public int getColumnCount() { return columns.length; }
            @Override public String getColumnName(int col) { return columns[col]; }
            @Override public Object getValueAt(int row, int col) {
                ClusterRow r = rows.get(row);
                return switch (col) {
                    case 0 -> r.component(); case 1 -> r.workerId() == null ? "-" : r.workerId();
                    case 2 -> r.port(); case 3 -> r.local(); case 4 -> r.cluster(); case 5 -> r.role();
                    case 6 -> r.jac() == null ? "-" : r.jac(); default -> "";
                };
            }
        }
    }

    private static final class JobRow {
        final String id, type;
        int progress;
        String status = "QUEUED", result = "";
        JobRow(String id, String type) { this.id = id; this.type = type; }
    }

    private static final class JobTableModel extends AbstractTableModel {
        private final String[] columns = {"Job ID", "Type", "Progress", "Status", "Result"};
        private final List<JobRow> rows = new ArrayList<>();

        void add(String id, String type) { rows.add(new JobRow(id, type)); fireTableRowsInserted(rows.size() - 1, rows.size() - 1); }
        String jobIdAt(int row) { return rows.get(row).id; }
        void update(JobProgress progress) {
            for (int i = 0; i < rows.size(); i++) if (rows.get(i).id.equals(progress.getJobId())) {
                JobRow r = rows.get(i);
                r.progress = progress.getOverallPercent();
                r.status = statusName(progress.getStatus());
                r.result = progress.getStatus() == JobStatus.JOB_COMPLETED ? String.valueOf(progress.getResult())
                        : progress.getStatus() == JobStatus.JOB_FAILED ? progress.getMessage() : "";
                fireTableRowsUpdated(i, i);
                return;
            }
        }
        @Override public int getRowCount() { return rows.size(); }
        @Override public int getColumnCount() { return columns.length; }
        @Override public String getColumnName(int col) { return columns[col]; }
        @Override public Class<?> getColumnClass(int col) { return col == 2 ? Integer.class : String.class; }
        @Override public Object getValueAt(int row, int col) {
            JobRow r = rows.get(row);
            return switch (col) { case 0 -> r.id; case 1 -> r.type; case 2 -> r.progress;
                case 3 -> r.status; case 4 -> r.result; default -> ""; };
        }
    }

    private static final class ProgressRenderer extends JProgressBar implements TableCellRenderer {
        ProgressRenderer() { super(0, 100); setStringPainted(true); }
        @Override public Component getTableCellRendererComponent(JTable table, Object value, boolean selected,
                                                                  boolean focused, int row, int column) {
            setValue(value instanceof Number n ? n.intValue() : 0);
            return this;
        }
    }
}
