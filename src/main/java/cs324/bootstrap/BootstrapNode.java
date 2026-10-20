package cs324.bootstrap;

import io.grpc.Server;
import io.grpc.ServerBuilder;

public final class BootstrapNode {
    private BootstrapNode() {}

    public static void main(String[] args) throws Exception {
        int port = args.length > 0 ? Integer.parseInt(args[0]) : 50050;
        BootstrapServiceImpl service = new BootstrapServiceImpl();
        Server server = ServerBuilder.forPort(port).addService(service).build().start();

        System.out.printf("Bootstrap running on gRPC port %d (worker timeout %d ms)%n",
                port, BootstrapServiceImpl.WORKER_TIMEOUT_MS);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            service.shutdown();
            server.shutdownNow();
        }));
        server.awaitTermination();
    }
}