package edu.kit.kastel.formal.bloatcache;

import java.io.IOException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;

public class StartServer {
    public static void main(String[] args) throws IOException {
        Server server = new Server(8081, "localhost");
        server.listen();
        ForkJoinPool.commonPool().awaitQuiescence(1, TimeUnit.SECONDS);
    }
}
