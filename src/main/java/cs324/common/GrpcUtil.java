package cs324.common;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.concurrent.TimeUnit;

public final class GrpcUtil {
    private GrpcUtil() {}

    public static ManagedChannel channel(String host, int port) {
        return ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
    }

    public static void shutdown(ManagedChannel channel) {
        if (channel == null) return;
        channel.shutdown();
        try {
            if (!channel.awaitTermination(500, TimeUnit.MILLISECONDS)) channel.shutdownNow();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            channel.shutdownNow();
        }
    }
}
