package edu.kit.kastel.formal.bloatcache;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.PushbackInputStream;
import java.math.BigInteger;
import java.net.Socket;
import java.util.Arrays;
import java.util.List;

import static edu.kit.kastel.formal.bloatcache.Util.checkArguments;
import static edu.kit.kastel.formal.bloatcache.Util.parseInt;

/**
 * @author Alexander Weigl
 * @version 1 (14.03.23)
 */
public class CommandHandling implements Runnable {
    private final Socket client;
    private final PushbackInputStream in;
    private final PrintWriter out;

    private final ServerData data;

    public CommandHandling(ServerData data, Socket clientSocket) throws IOException {
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
                    var args = Util.readArguments(in);
                    //if ("END".equals(args[0])) break;
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


    private void sendError() {
        out.format("ERROR\r\n");
    }

    private void handleCommand(List<byte[]> args) throws IOException {
        String command = new String(args.get(0));

        switch (command) {
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
    private void handleTouchCommand(List<byte[]> args) {
        checkArguments(args, "touch", "K", "T", "[noreply]");
        var noreply = isNoreply(args);
        var key = new Entry.Key(args.get(1));
        var exptime = Util.expirationTime(args.get(2));

        var d = data.get(key);
        if (d != null) {
            d.expirationDate = exptime;
            if (!noreply) sendTouched();
        } else {
            if (!noreply) sendNotFound();
        }
    }

    private void sendTouched() {
        out.format("TOUCHED\r\n");
    }

    /**
     * Check And Set (or Compare And Swap). An operation that stores data, but only if no one else has updated
     * the data since you read it last. Useful for resolving race conditions on updating cache data.
     */
    public void handleCasCommand(List<byte[]> args) throws IOException {
        checkArguments(args, "cas", "K", "F", "T", "I", "C", "[noreply]");
        var key = new Entry.Key(args.get(1));
        var flags = parseInt(args.get(2));
        var exptime = parseInt(args.get(3));
        var bytes = parseInt(args.get(4));
        var cas = Util.parseLongNumber(args.get(5));
        var noreply = isNoreply(args);
        var data = Util.readLineExactly(in, bytes);

        var currentEntry = this.data.get(key);

        if (currentEntry != null) {
            if (currentEntry.cas == cas) {
                currentEntry.update(data, exptime, flags);
                if (!noreply) sendStored();
                return;
            }
            if (!noreply) sendNotStored();
        }
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
    private void handleDeleteCommand(List<byte[]> args) {
        checkArguments(args, "delete", "K", "[noreply]");
        var noreply = isNoreply(args);
        var key = args.get(1);
        var val = data.delete(new Entry.Key(key));

        if (!noreply) {
            if (val) {
                out.format("DELETED\r\n");
            } else {
                out.format("NOT_FOUND\r\n");
            }
        }
    }

    private boolean isNoreply(List<byte[]> args) {
        return args.get(args.size() - 1).equals("noreply");
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
    private void handleGatCommand(List<byte[]> args) {
        checkArguments(args, "gat", "T", "K*");
        var time = Util.expirationTime(args.get(1));
        for (int i = 2; i < args.size(); i++) {
            final var key = new Entry.Key(args.get(i));
            var value = data.get(key);
            if (value != null) {
                sendValue(value.key.value, value.flags, value.value, value.cas);
                value.expirationDate = time;
            }
        }
        sendEnd();
    }

    private void sendEnd() {
        out.format("END\r\n");
    }

    private void handleManipCommand(List<byte[]> args) throws IOException {
        // <command name> <key> <flags> <exptime> <bytes>
        checkArguments(args, "set|replace|add|append|prepend", "K", "F", "T", "I");
        var key = new Entry.Key(args.get(1));
        var flags = parseInt(args.get(2));
        var exptime = parseInt(args.get(3));
        var bytes = parseInt(args.get(4));
        var noreply = isNoreply(args);
        var data = Util.readLineExactly(in, bytes);

        final var cmd = args.get(0);
        var set = Util.equals("set", cmd);
        var replace = Util.equals("replace", cmd);
        var add = Util.equals("add", cmd);
        var append = Util.equals("append", cmd);
        var prepend = Util.equals("prepend", cmd);

        var currentEntry = this.data.get(key);

        if (replace) {
            if (currentEntry != null) {
                currentEntry.update(data, exptime, flags);
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
                currentEntry.update(newValue, exptime, flags);
                if (!noreply) sendStored();
            } else {
                if (!noreply) sendNotStored();
                return;
            }
        }

        if (prepend) {
            if (currentEntry != null) {
                byte[] newValue = concatArray(data, currentEntry.value);
                currentEntry.update(newValue, exptime, flags);
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
    void handleIncrDecrCommand(List<byte[]> args) {
        checkArguments(args, "incr|decr", "K", "I");
        var noreply = isNoreply(args);
        var entry = data.get(new Entry.Key(args.get(1)));
        if (entry == null) {
            sendNotFound();
            return;
        }

        assert (entry.value.length <= 8); // should look like a 64bit integer

        //Normally we should use BigInteger to receive true 64-bit unsigned ints.
        var param = Util.parseLongNumber(args.get(2));
        var value = Util.parseLongNumber(entry.value);
        var mask = BigInteger.valueOf(-1); // 64bit mask

        if (Util.equals("incr", args.get(0))) {
            value = value + param;
        }

        if (Util.equals("decr", args.get(0))) {
            value = value + param;
            if (value < 0) {
                value = 0;
            }
        }

        entry.value = ("" + value).getBytes();

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
    private void handleGetCommand(List<byte[]> args) {
        checkArguments(args, "get", "K*");
        for (int i = 1; i < args.size(); i++) {
            final var key = new Entry.Key(args.get(i));
            var value = data.get(key);
            if (value != null) {
                sendValue(value.key.value, value.flags, value.value, value.cas);
            }
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
