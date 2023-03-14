package edu.kit.kastel.formal;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.math.BigInteger;
import java.net.Socket;

/**
 * @author Alexander Weigl
 * @version 1 (14.03.23)
 */
public class Client implements AutoCloseable {
    private final InputStream in;
    private final PrintStream out;
    private final Socket socket;


    public Client() throws IOException {
        this.socket = new Socket("localhost", 8081);
        out = new PrintStream(socket.getOutputStream());
        in = socket.getInputStream();
    }

    public static void main(String[] args) throws IOException {
        try (var client = new Client()) {
            client.set("test", "abc");
            client.incr("abc", 10, false);
            client.decr("abc", 10);
        }
    }

    public void set(String key, String value) {
        out.format("set %s %d\r\n", key, value);
    }

    private byte[] readLine(long length) throws IOException {
        var out = new ByteArrayOutputStream();
        for (; length >= 0; length--) {
            var c = in.read();
            if (c == -1) {
                throw new RuntimeException("Channel closed before end of data reached");
            }
            out.write(c);
        }

        assert (in.read() == '\r');
        assert (in.read() == '\n');
        return out.toByteArray();
    }

    public String get(String key) {
        out.format("set %s %d\r\n", key);
        return key;
    }

    private BigInteger incr(String key) throws IOException {
        return incr(key, 1, false);
    }

    private BigInteger incr(String key, int value) throws IOException {
        return incr(key, value, false);
    }

    private BigInteger incr(String key, int value, boolean noreply) throws IOException {
        out.format("incr %s %d %s\r\n", key, value, noreply ? "noreply" : "");
        if (!noreply) {
            var resp = readLine(-1);
        }
        return null;
    }

    private void decr(String key, int value) {
        out.format("decr %s %d\r\n", key, value);
    }

    public void close() throws IOException {
        out.close();
        in.close();
        socket.close();
    }
}
