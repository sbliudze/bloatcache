package edu.kit.kastel.formal.bloatcache;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.math.BigInteger;
import java.net.Socket;
import java.util.Arrays;

/**
 * @author Alexander Weigl
 * @version 1 (14.03.23)
 */
public class Client implements AutoCloseable {
    private final InputStream in;
    private final PrintStream out;
    private final Socket socket;

    public Client(Socket socket) throws IOException {
        this.socket = socket;
        out = new PrintStream(socket.getOutputStream(), true);
        in = socket.getInputStream();
    }

    public Client(String hostname, int port) throws IOException {
        this(new Socket(hostname, port));
    }

    public Client() throws IOException {
        this(new Socket("localhost", 8081));
    }

    public void set(String key, String value) throws IOException {
        set(key, value, 0, 0);
    }

    public void add(String key, String value) throws IOException {
        add(key, value, 0, 0);
    }

    public void replace(String key, String value) throws IOException {
        replace(key, value, 0, 0);
    }

    public void prepend(String key, String value) throws IOException {
        prepend(key, value, 0, 0);
    }

    public void append(String key, String value) throws IOException {
        append(key, value, 0, 0);
    }


    public void set(String key, String value, int flags, int exptime) throws IOException {
        manipulationCommand("set", key, value, flags, exptime);
    }

    public void replace(String key, String value, int flags, int exptime) throws IOException {
        manipulationCommand("replace", key, value, flags, exptime);
    }

    public void append(String key, String value, int flags, int exptime) throws IOException {
        manipulationCommand("append", key, value, flags, exptime);
    }

    public void prepend(String key, String value, int flags, int exptime) throws IOException {
        manipulationCommand("prepend", key, value, flags, exptime);
    }

    public void add(String key, String value, int flags, int exptime) throws IOException {
        manipulationCommand("add", key, value, flags, exptime);
    }


    private void manipulationCommand(String command, String key, String value, int flags, int exptime) throws IOException {
        var b = value.getBytes();
        out.format("%s %s %d %d %d\r\n", command, key, flags, exptime, b.length);
        out.write(b);
        out.format("\r\n");
        checkForError();
    }

    private void checkForError() throws IOException {
        byte[] line = Util.readLine(in);
        System.out.println(new String(line));
    }

    private byte[] readLine(long length) throws IOException {
        return Util.readLineExactly(in, length);
    }

    /**
     * <pre>
     * VALUE <key> <flags> <bytes> [<cas unique>]\r\n
     * <data block>\r\n
     * </pre>
     *
     * @return
     */
    private String readValue() throws IOException {
        var args = Util.readArguments(in);
        var result = new String(args.get(0));
        switch (result) {
            case "VALUE":
                long length = Long.parseLong(new String(args.get(3)));
                var value = Util.readLineExactly(in, length);
                return new String(value);
            default:
                throw new RuntimeException("EXCEPTION: " + result);
        }
    }


    public String[] gets(String... key) throws IOException {
        out.format("get");
        for (String k : key)
            out.format(" %s", k);
        out.format("\r\n");
        Arrays.fill(key, readValue());
        return key;
    }

    public String get(String key) throws IOException {
        out.format("get %s\r\n", key);
        return readValue();
    }

    public String[] gats(int exptime, String... key) throws IOException {
        out.format("get %d", exptime);
        for (String k : key)
            out.format(" %s", k);
        out.format("\r\n");
        Arrays.fill(key, readValue());
        return key;
    }

    public String gat(int exptime, String key) throws IOException {
        out.format("gat %d %s\r\n", exptime, key);
        return readValue();
    }


    private BigInteger incr(String key) throws IOException {
        return incr(key, 1, false);
    }

    private BigInteger incr(String key, int value) throws IOException {
        return incr(key, value, false);
    }

    public BigInteger incr(String key, int value, boolean noreply) throws IOException {
        out.format("incr %s %d %s\r\n", key, value, noreply ? "noreply" : "");
        if (!noreply) {
            var resp = readLine(-1);
        }
        return null;
    }

    public void decr(String key, int value) {
        out.format("decr %s %d\r\n", key, value);
    }

    public void end() {
        out.format("END\r\n");
    }

    public void close() throws IOException {
        end();
        out.close();
        in.close();
        socket.close();
    }
}
