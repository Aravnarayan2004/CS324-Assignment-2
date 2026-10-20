package cs324.worker;

import io.grpc.Server;
import io.grpc.ServerBuilder;

public final class WorkerNode {
    private WorkerNode() {}

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println("Usage: WorkerNode <ID> <PORT> [BOOTSTRAP_HOST] [BOOTSTRAP_PORT] [ADVERTISED_HOST]");
            return;
        }

        
        int id = Integer.parseInt(args[0]);
        int port = Integer.parseInt(args[1]);
        String bootstrapHost = args.length > 2 ? args[2] : "127.0.0.1";
        int bootstrapPort = args.length > 3 ? Integer.parseInt(args[3]) : 50050;
        String host = args.length > 4 ? args[4] : "127.0.0.1";

        WorkerRuntime runtime = new WorkerRuntime(id, host, port, bootstrapHost, bootstrapPort);
        Server server = ServerBuilder.forPort(port)
                .addService(new WorkerGrpcService(runtime))
                .addService(new CoordinatorGrpcService(runtime))
                .build().start();
        try {
            runtime.start();
        } catch (Exception e) {
            server.shutdownNow();
            throw e;
        }

        System.out.printf("Worker %d running at %s:%d | Bootstrap %s:%d%n",
                id, host, port, bootstrapHost, bootstrapPort);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            runtime.shutdown();
            server.shutdownNow();
        }));
        server.awaitTermination();
    }
}
