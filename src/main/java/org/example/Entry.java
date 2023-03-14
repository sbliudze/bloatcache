package org.example;

/**
 * A key (arbitrary string up to 250 bytes in length. No space or newlines for ASCII mode)
 * A 32bit "flag" value
 * An expiration time, in seconds. '0' means never expire. Can be up to 30 days. After 30 days, is treated as a unix timestamp of an exact date.
 * A 64bit "CAS" value, which is kept unique.
 * Arbitrary data
 * @author Alexander Weigl
 * @version 1 (14.03.23)
 */
public class Entry {
    //@invariant key.length <= 250;
    byte[] key;

    int flag;

    int expirationDate;

    long cas;
}
