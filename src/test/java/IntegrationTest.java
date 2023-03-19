import edu.kit.kastel.formal.bloatcache.Client;
import edu.kit.kastel.formal.bloatcache.Server;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;

/**
 * @author Alexander Weigl
 * @version 1 (19.03.23)
 */
public class IntegrationTest {

    private Server server;

    @BeforeEach
    void setUp() throws IOException {
        server = new Server(8081, "localhost");
        CompletableFuture.runAsync(() -> {
            try {
                server.listen();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @AfterEach
    void destroy() throws Exception {
        ForkJoinPool.commonPool().awaitQuiescence(1, TimeUnit.SECONDS);
        server.close();
    }

    @Test
    void test1() throws IOException {
        try (var client = new Client()) {
            client.set("abc", "def");
            var val = client.get("abc");
            Assertions.assertEquals("def", val);
        }
    }

    @Test
    void test2() throws IOException {
        try (var client = new Client()) {
            client.set("test", "abc");
            client.incr("abc", 10, false);
            client.decr("abc", 10);
        }
    }
}
