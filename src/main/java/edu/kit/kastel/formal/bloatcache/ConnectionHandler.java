package edu.kit.kastel.formal.bloatcache;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.PushbackInputStream;
import java.math.BigInteger;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static edu.kit.kastel.formal.bloatcache.Util.checkArguments;

/**
 * @author Alexander Weigl
 * @version 1 (14.03.23)
 */
public class ConnectionHandler implements Runnable {
    private final Socket client;
    private final PushbackInputStream in;
    private final PrintWriter out;

    private final ServerData data;

    public ConnectionHandler(ServerData data, Socket clientSocket) throws IOException {
        this.data = data;
        this.client = clientSocket;
        in = new PushbackInputStream(clientSocket.getInputStream());
        out = new PrintWriter(clientSocket.getOutputStream(), true);
    }

    @Override
    public void run() {
        try {
            while (true) {
                try {
                    String[] args = readCommandLine();
                    if ("END".equals(args[0])) break;
                    handleCommand(args);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        } finally {
            try {
                in.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            out.close();
            try {
                client.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private byte[] readLine(long length) throws IOException {
        var out = new ByteArrayOutputStream();
        for (; length > 0; length--) {
            var c = in.read();
            if (c == -1) {
                sendError();
                throw new RuntimeException("Channel closed before end of data reached");
            }
            out.write(c);
        }

        assert (in.read() == '\r');
        assert (in.read() == '\n');
        return out.toByteArray();
    }

    private void sendError() {
        out.format("ERROR\r\n");
    }

    private String[] readCommandLine() throws IOException {
        List<String> args = new ArrayList<>(16);
        StringBuilder current = new StringBuilder();

        while (true) {
            var c = in.read();
            if (c == '\r') {
                var c2 = in.read();
                if (c2 == '\n') {
                    args.add(current.toString());
                    break;
                } else {
                    in.unread(c2);
                }
            }
            if (c == ' ') {
                args.add(current.toString());
                current = new StringBuilder();
            } else {
                current.append((char) c);
            }
        }
        return args.toArray(new String[0]);
    }


    private void handleCommand(String[] args) throws IOException {
        switch (args[0]) {
            case "get":
            case "gets":
                handleGetCommand(args);
                break;
            case "gat":
            case "gats":
                handleGatCommand(args);
                break;

            case "set":
            case "add":
            case "replace":
            case "append":
            case "prepend":
                handleManipCommand(args);
                break;
            case "cas":
                handleCasCommand(args);
                break;
            case "incr":
            case "decr":
                handleIncrDecrCommand(args);
                break;
            case "delete":
                handleDeleteCommand(args);
                break;
            case "touch":
                handleTouchCommand(args);
                break;
            case "flush_all":
                assert false;
                break;
        }
    }

    /**
     * Touch
     * -----
     * <p>
     * The "touch" command is used to update the expiration time of an existing item
     * without fetching it.
     * <p>
     * touch <key> <exptime> [noreply]\r\n
     * <p>
     * - <key> is the key of the item the client wishes the server to touch
     * <p>
     * - <exptime> is expiration time. Works the same as with the update commands
     * (set/add/etc). This replaces the existing expiration time. If an existing
     * item were to expire in 10 seconds, but then was touched with an
     * expiration time of "20", the item would then expire in 20 seconds.
     * <p>
     * - "noreply" optional parameter instructs the server to not send the
     * reply.  See the note in Storage commands regarding malformed
     * requests.
     * <p>
     * The response line to this command can be one of:
     * <p>
     * - "TOUCHED\r\n" to indicate success
     * <p>
     * - "NOT_FOUND\r\n" to indicate that the item with this key was not
     * found.
     *
     * @param args
     */
    private void handleTouchCommand(String[] args) {

    }

    /**
     * cas
     * Check And Set (or Compare And Swap). An operation that stores data, but only if no one else has updated the data since you read it last. Useful for resolving race conditions on updating cache data.
     */
    public void handleCasCommand(String[] args) {

    }


    /**
     * <code><pre>
     * Deletion
     * --------
     *
     * The command "delete" allows for explicit deletion of items:
     *
     * delete <key> [noreply]\r\n
     *
     * - <key> is the key of the item the client wishes the server to delete
     *
     * - "noreply" optional parameter instructs the server to not send the
     * reply.  See the note in Storage commands regarding malformed
     * requests.
     *
     * The response line to this command can be one of:
     *
     * - "DELETED\r\n" to indicate success
     *
     * - "NOT_FOUND\r\n" to indicate that the item with this key was not
     * found.
     *
     * See the "flush_all" command below for immediate invalidation
     * of all existing items.
     *
     * </pre>
     * </code>
     *
     * @param args
     */
    private void handleDeleteCommand(String[] args) {
        checkArguments(args, "delete", "K", "[noreply]");
        var noreply = isNoreply(args);
        var key = args[1];
        var val = data.delete(new Entry.Key(key));

        if (!noreply) {
            if (val) {
                out.format("DELETED\r\n");
            } else {
                out.format("NOT_FOUND\r\n");
            }
        }
    }

    private boolean isNoreply(String[] args) {
        return args[args.length - 1].equals("noreply");
    }

    /**
     * <code><pre>Get And Touch
     * -------------
     *
     * The "gat" and "gats" commands are used to fetch items and update the
     * expiration time of an existing items.
     *
     * gat <exptime> <key>*\r\n
     * gats <exptime> <key>*\r\n
     *
     * - <exptime> is expiration time.
     *
     * - <key>* means one or more key strings separated by whitespace.
     *
     * After this command, the client expects zero or more items, each of
     * which is received as a text line followed by a data block. After all
     * the items have been transmitted, the server sends the string
     *
     * "END\r\n"
     *
     * to indicate the end of response.
     *
     * Each item sent by the server looks like this:
     *
     * VALUE <key> <flags> <bytes> [<cas unique>]\r\n
     * <data block>\r\n
     *
     * - <key> is the key for the item being sent
     *
     * - <flags> is the flags value set by the storage command
     *
     * - <bytes> is the length of the data block to follow, *not* including
     *   its delimiting \r\n
     *
     * - <cas unique> is a unique 64-bit integer that uniquely identifies
     *   this specific item.
     *
     * - <data block> is the data for this item.</pre></code>
     *
     * @param args
     */
    private void handleGatCommand(String[] args) {
        checkArguments(args, "gat", "T", "K*");
        var time = Util.expirationTime(Integer.parseInt(args[1]));
        for (int i = 2; i < args.length; i++) {
            final var key = new Entry.Key(args[i]);
            var value = data.get(key);
            if (value != null) {
                sendValue(value.key.value, value.flags, value.value, value.cas);
                value.expirationDate = time;
            } else {
                sendNotFound();
            }
        }
        sendEnd();

    }

    private void sendEnd() {
        out.format("END\r\n");
    }

    private void handleManipCommand(String[] args) throws IOException {
        // <command name> <key> <flags> <exptime> <bytes>
        checkArguments(args, "set|replace|add|append|prepend", "K", "F", "T", "I");
        var key = new Entry.Key(args[1]);
        var flags = Integer.parseInt(args[2]);
        var exptime = Integer.parseInt(args[3]);
        var bytes = Integer.parseInt(args[4]);
        var noreply = isNoreply(args);
        var data = readLine(bytes);

        var set = "set".equals(args[0]);
        var replace = "replace".equals(args[0]);
        var add = "add".equals(args[0]);
        var append = "append".equals(args[0]);
        var prepend = "prepend".equals(args[0]);

        var currentEntry = this.data.get(key);

        if (replace) {
            if (currentEntry != null) {
                currentEntry.value = data;
                //TODO set everything
                if (!noreply) sendStored();
            } else {
                if (!noreply) sendNotStored();
                return;
            }
        }

        if (add) {
            if (currentEntry != null) {
                if (!noreply) sendNotStored();
            } else {
                if (this.data.insert(new Entry(key, flags, exptime, data))) {
                    if (!noreply) sendStored();
                } else {
                    if (!noreply) sendNotStored();
                }
            }
        }

        if (set) {
            if (this.data.insert(new Entry(key, flags, exptime, data))) {
                if (!noreply) sendStored();
            } else {
                if (!noreply) sendNotStored();
            }
        }

        if (append) {
            if (currentEntry != null) {
                byte[] newValue = concatArray(currentEntry.value, data);
                currentEntry.value = newValue;
                //TODO set everything
                if (!noreply) sendStored();
            } else {
                if (!noreply) sendNotStored();
                return;
            }
        }

        if (prepend) {
            if (currentEntry != null) {
                currentEntry.value = concatArray(data, currentEntry.value);
                //TODO set everything
                if (!noreply) sendStored();
            } else {
                if (!noreply) sendNotStored();
                return;
            }
        }

    }

    private byte[] concatArray(byte[] a, byte[] b) {
        var target = Arrays.copyOf(a, a.length + b.length);
        for (int i = a.length, j = 0; j < b.length; i++, j++) {
            target[i] = b[j];
        }
        return target;
    }

    /**
     * "STORED\r\n", to indicate success.
     */
    public void sendStored() {
        out.format("STORED\r\n");
    }

    /**
     * - "NOT_STORED\r\n" to indicate the data was not stored, but not
     * because of an error. This normally means that the
     * condition for an "add" or a "replace" command wasn't met.
     */
    public void sendNotStored() {
        out.format("NOT_STORED\r\n");
    }

    /**
     * "EXISTS\r\n" to indicate that the item you are trying to store with a "cas" command
     * has been modified since you last fetched it.
     */
    public void sendExists() {
        out.format("EXISTS\r\n");
    }

    /**
     * "NOT_FOUND\r\n" to indicate that the item you are trying to store
     * with a "cas" command did not exist.
     */
    public void sendNotFound() {
        out.format("NOT_FOUND\r\n");
        out.flush();
    }


    /**
     * <code><pre>
     * Increment/Decrement
     * -------------------
     *
     * Commands "incr" and "decr" are used to change data for some item
     * in-place, incrementing or decrementing it. The data for the item is
     * treated as decimal representation of a 64-bit unsigned integer.  If
     * the current data value does not conform to such a representation, the
     * incr/decr commands return an error (memcached <= 1.2.6 treated the
     * bogus value as if it were 0, leading to confusion). Also, the item
     * must already exist for incr/decr to work; these commands won't pretend
     * that a non-existent key exists with value 0; instead, they will fail.
     *
     * The client sends the command line:
     *
     * incr <key> <value> [noreply]\r\n
     *
     * or
     *
     * decr <key> <value> [noreply]\r\n
     *
     * - <key> is the key of the item the client wishes to change
     *
     * - <value> is the amount by which the client wants to increase/decrease
     * the item. It is a decimal representation of a 64-bit unsigned integer.
     *
     * - "noreply" optional parameter instructs the server to not send the
     * reply.  See the note in Storage commands regarding malformed
     * requests.
     *
     * The response will be one of:
     *
     * - "NOT_FOUND\r\n" to indicate the item with this value was not found
     *
     * - <value>\r\n , where <value> is the new value of the item's data,
     * after the increment/decrement operation was carried out.
     *
     * Note that underflow in the "decr" command is caught: if a client tries
     * to decrease the value below 0, the new value will be 0.  Overflow in
     * the "incr" command will wrap around the 64 bit mark.
     *
     * Note also that decrementing a number such that it loses length isn't
     * guaranteed to decrement its returned length.  The number MAY be
     * space-padded at the end, but this is purely an implementation
     * optimization, so you also shouldn't rely on that.
     * </pre></code>
     */
    void handleIncrDecrCommand(String[] args) {
        checkArguments(args, "incr|decr", "K", "I");
        var noreply = isNoreply(args);
        var entry = data.get(new Entry.Key(args[1]));
        if (entry == null) {
            sendNotFound();
            return;
        }

        assert (entry.value.length <= 8); // should look like a 64bit integer

        var param = BigInteger.valueOf(Long.parseLong(args[2]));
        var value = new BigInteger(entry.value);
        var mask = BigInteger.valueOf(-1); // 64bit mask

        if ("incr".equals(args[0])) {
            value = value.add(param).and(mask);
            entry.value = value.toByteArray();
        }

        if ("decr".equals(args[0])) {
            value = value.add(param);
            if (value.compareTo(BigInteger.ZERO) < 0) {
                value = BigInteger.ZERO;
            }
            entry.value = value.toByteArray();
        }

        if (!noreply) {
            out.format("%s\r\n", value);
        }
    }

    /**
     * <code><pre>
     * Retrieval command:
     * ------------------
     *
     * The retrieval commands "get" and "gets" operate like this:
     *
     * get <key>*\r\n
     * gets <key>*\r\n
     *
     * - <key>* means one or more key strings separated by whitespace.
     *
     * After this command, the client expects zero or more items, each of
     * which is received as a text line followed by a data block. After all
     * the items have been transmitted, the server sends the string
     *
     * "END\r\n"
     *
     * to indicate the end of response.
     *
     * Each item sent by the server looks like this:
     *
     * VALUE <key> <flags> <bytes> [<cas unique>]\r\n
     * <data block>\r\n
     *
     * - <key> is the key for the item being sent
     *
     * - <flags> is the flags value set by the storage command
     *
     * - <bytes> is the length of the data block to follow, *not* including
     * its delimiting \r\n
     *
     * - <cas unique> is a unique 64-bit integer that uniquely identifies
     * this specific item.
     *
     * - <data block> is the data for this item.
     *
     * If some of the keys appearing in a retrieval request are not sent back
     * by the server in the item list this means that the server does not
     * hold items with such keys (because they were never stored, or stored
     * but deleted to make space for more items, or expired, or explicitly
     * deleted by a client).
     * </pre>
     * </code>
     *
     * @param args
     */
    private void handleGetCommand(String[] args) {
        checkArguments(args, "get", "K*");
        for (int i = 1; i < args.length; i++) {
            final var key = new Entry.Key(args[i]);
            var value = data.get(key);
            if (value != null) {
                sendValue(value.key.value, value.flags, value.value, value.cas);
            } else {
                sendNotFound();
            }
            System.out.println("REQUEST HANDLED");
        }
    }

    /**
     * Each item sent by the server looks like this:
     *
     * <pre>
     * VALUE <key> <flags> <bytes> [<cas unique>]\r\n
     * <data block>\r\n
     * </pre>
     *
     * @param key
     * @param flags
     * @param v
     */
    private void sendValue(byte[] key, int flags, byte[] v, Long cas) {
        out.format("VALUE ");
        for (byte b : key) out.write(b);
        out.format(" %d %d%s\r\n", flags, v.length, cas == null ? "" : " " + cas);
        for (byte b : v) out.write(b);
        out.format("\r\n");
        out.flush();
    }

}
