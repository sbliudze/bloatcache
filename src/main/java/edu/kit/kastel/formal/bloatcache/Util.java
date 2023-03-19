package edu.kit.kastel.formal.bloatcache;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PushbackInputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author Alexander Weigl
 * @version 1 (19.03.23)
 */
public class Util {

    public static byte[] readLine(InputStream in) throws IOException {
        var pushback = new PushbackInputStream(in);
        var out = new ByteArrayOutputStream();
        while (true) {
            var c = pushback.read();
            if (c == -1) {
                break;
            }

            if (c == '\r') {
                var c2 = pushback.read();
                if (c2 == '\n') {
                    break;
                } else {
                    pushback.unread(c2);
                }
            }
            out.write(c);
        }
        return out.toByteArray();
    }

    public static byte[] readLineMax(InputStream input, long length) throws IOException {
        var in = new PushbackInputStream(input);
        var out = new ByteArrayOutputStream();
        for (; length >= 0; length--) {
            var c = in.read();
            if (c == -1) {
                throw new RuntimeException("Channel closed before end of data reached");
            }

            if (c == '\r') {
                var c2 = in.read();
                if (c2 == '\n') {
                    break;
                } else {
                    in.unread(c2);
                }
            }
            out.write(c);
        }
        return out.toByteArray();
    }

    public static byte[] readLineExactly(InputStream in, long length) throws IOException {
        var out = new ByteArrayOutputStream();
        for (; length > 0; length--) {
            var c = in.read();
            if (c == -1) {
                throw new RuntimeException("Channel closed before end of data reached");
            }
            out.write(c);
        }

        var r = in.read();
        var n = in.read();
        if (r != '\r' || n != '\n')
            throw new AssertionError("Unexpected character. Expected Newline. Instead: " + r + " " + n);
        return out.toByteArray();
    }

    public static int RELATIVE_TIME_LIMIT = 60 * 60 * 24 * 30;

    public static List<byte[]> readArguments(InputStream in) throws IOException {
        byte[] line = readLine(in);
        List<byte[]> seq = new ArrayList<>();
        var out = new ByteArrayOutputStream();
        for (byte b : line) {
            if (b == ' ') {
                seq.add(out.toByteArray());
                out.reset();
            } else {
                out.write(b);
            }
        }
        seq.add(out.toByteArray());
        return seq;
    }

    private static byte[] readArgument(PushbackInputStream in) throws IOException {
        var out = new ByteArrayOutputStream();
        while (true) {
            var c = in.read();
            if (c == -1) break;
            if (c == '\r') {
                var c2 = in.read();
                if (c2 == '\n')
                    break;
                else {
                    //restore \r\n completely to handle optional parameters
                    in.unread(c2);
                    in.unread(c);
                }
            }
            if (c == ' ') break;
            out.write(c);
        }
        return out.toByteArray();
    }

    private static Object handleArgument(byte[] arg, String type) {
        if (type.startsWith("<")) {
            type = type.substring(1);
            return parseArgument(arg, type);
        } else if (type.startsWith("[")) {
            type = type.substring(1);
            if (arg.length == 0) return null;
            return parseArgument(arg, type);
        } else {
            assert (type.equals(new String(arg)));
            return new String(arg);
        }
    }

    private static Object parseArgument(byte[] arg, String type) {
        switch (type) {
            case "key":
                return new String(arg);
            case "flags":
                return Integer.parseInt(new String(arg));
            case "cas":
            case "long":
                return Long.parseLong(new String(arg));
            default:
                throw new RuntimeException("Unexpected type for byte array: " + new String(arg));
        }
    }

    /**
     * Expiration times
     * ----------------
     * <p>
     * Some commands involve a client sending some kind of expiration time
     * (relative to an item or to an operation requested by the client) to
     * the server. In all such cases, the actual value sent may either be
     * Unix time (number of seconds since January 1, 1970, as a 32-bit
     * value), or a number of seconds starting from current time. In the
     * latter case, this number of seconds may not exceed 60*60*24*30 (number
     * of seconds in 30 days); if the number sent by a client is larger than
     * that, the server will consider it to be real Unix time value rather
     * than an offset from current time.
     * <p>
     * Note that a TTL of 1 will sometimes immediately expire. Time is internally
     * updated on second boundaries, which makes expiration time roughly +/- 1s.
     * This more proportionally affects very low TTL's.
     *
     * @param exptime
     * @return
     */
    public static int expirationTime(int exptime) {
        if (exptime < RELATIVE_TIME_LIMIT) {
            return (int) (System.currentTimeMillis() / 1000) + exptime;
        }
        return exptime;
    }

    public static void checkArguments(String[] args, String... specifier) {
        if (!checkArguments(0, 0, args, specifier)) {
            System.out.format("%s does not match %s\n",
                    Arrays.toString(args), Arrays.toString(specifier));
            throw new RuntimeException("Arguments unexpected");
        }
    }

    public static boolean checkArgument(String arg, String exp) {
        if (exp.endsWith("*")) {
            exp = exp.substring(0, exp.length() - 1);
        }

        if (exp.contains("|")) {
            String[] allowed = exp.split("\\|");
            return Arrays.stream(allowed).anyMatch(it -> checkArgument(arg, it));
        }

        if (exp.startsWith("[")) {
            if (arg == null) {
                return true;
            }
            exp = exp.substring(1, exp.length() - 1);
            return checkArgument(arg, exp);
        }

        switch (exp) {
            case "K":
                return arg.length() <= 250 && !arg.contains(" ");
            case "T": //TODO be more restrictive
            case "F": //TODO be more restrictive
            case "I":
                return arg.matches("[0-9]+");
        }

        if (exp.toLowerCase().equals(exp)) {
            return exp.equals(arg);
        }

        throw new RuntimeException("unknown expected argument " + exp);
    }

    public static boolean checkArguments(int posA, int posS, String[] args, String[] expected) {
        if (posA >= args.length) return true;
        if (posS >= expected.length) return true;

        var arg = args[posA];
        var exp = expected[posS];

        if (!checkArgument(arg, exp)) {
            return false;
        }

        if (!exp.endsWith("*")) posS += 1;
        return checkArguments(posA + 1, posS, args, expected);
    }
}
