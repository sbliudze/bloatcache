package edu.kit.kastel.formal.bloatcache;

import java.util.Arrays;

/**
 * A key (arbitrary string up to 250 bytes in length. No space or newlines for ASCII mode)
 * A 32bit "flag" value
 * An expiration time, in seconds. '0' means never expire. Can be up to 30 days. After 30 days, is treated as a unix timestamp of an exact date.
 * A 64bit "CAS" value, which is kept unique.
 * Arbitrary data
 *
 * @author Alexander Weigl
 * @version 1 (14.03.23)
 */
public class Entry {
    Key key;

    byte[] value;

    int flags;

    int expirationDate;

    long cas = 0;

    public Entry(String key, int flags, int exptime, byte[] data) {
        this(new Key(key), flags, exptime, data);
    }

    public Entry(Key key, int flags, int exptime, byte[] data) {
        this.key = key;
        this.flags = flags;
        this.expirationDate = exptime;
        this.value = data;
    }

    public void update(byte[] data, Integer exptime, Integer flags) {
        if (!Arrays.equals(data, value)) {
            cas++;
        }
        value = data;
        if (exptime != null) expirationDate = exptime;
        if (flags != null) this.flags = flags;
    }

    public static class Key {
        //@invariant  0 <= key.length <= 250;
        final byte[] value;

        public Key(byte[] value) {
            this.value = value;
        }

        public Key(String arg) {
            this(arg.getBytes());
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Key)) return false;
            Key key = (Key) o;
            return Arrays.equals(value, key.value);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(value);
        }
    }

    @Override
    public String toString() {
        return new String(value);
    }
}
