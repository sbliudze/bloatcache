package org.example;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.PushbackInputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

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
        out = new PrintWriter(clientSocket.getOutputStream());
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
                    in.unread(c);
                }
            }
            if (c == ' ') {
                args.add(current.toString());
                current = new StringBuilder();
            }
            current.append((char) c);
        }
        return args.toArray(new String[0]);
    }


    private void handleCommand(String[] args) {
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
                handleSetCommand(args);
                break;
            case "add":
                handleAddCommand(args);
                break;
            case "replace":
                handleReplaceCommand(args);
                break;
            case "append":
                handleAppendCommand(args);
                break;
            case "prepend":
                handlePrependCommand(args);
                break;

            case "delete":
                handleDeleteCommand(args);
                break;
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
    private void handleDeleteCommand(String[] args) {
        checkArguments(args, "delete", "K", "[noreply]");
        var noreply = isNoreply(args);
        var key = args[1];
        var val = data.value.remove(key);

        if (val == null) {
            out.format("NOT_FOUND\r\n");
        } else {
            out.format("DELETED\r\n");
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

    }

    private void handleGetsCommand(String[] args) {
    }

    private void handleGatsCommand(String[] args) {
    }

    private void handleSetCommand(String[] args) {

    }

    private void handleAddCommand(String[] args) {

    }

    private void handleReplaceCommand(String[] args) {

    }

    private void handleAppendCommand(String[] args) {

    }

    private void handlePrependCommand(String[] args) {

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
    void handleIncrDecrCommand(String args[]) {

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
            final var key = args[i];
            var value = data.value.get(key);
            if (value != null) {
                var flags = data.flags.get(key);

                out.format("VALUE %s %s %d %s\r\n",
                        key, flags, value.length(), "");

                out.format("%s\r\n", value);
            }
        }
    }

    private void checkArguments(String[] args, String... specifier) {
    }
}
