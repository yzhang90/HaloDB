/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */

package com.oath.halodb;

import com.google.common.primitives.Longs;

public class Utils {
    public static long roundUpToPowerOf2(long number) {
        return (number > 1) ? Long.highestOneBit((number - 1) << 1) : 1;
    }

    public static int getValueOffset(int recordOffset, byte[] key) {
        return recordOffset + Record.Header.HEADER_SIZE + key.length;
    }

    //TODO: probably belongs to Record.
    public static int getRecordSize(int keySize, int valueSize) {
        return keySize + valueSize + Record.Header.HEADER_SIZE;
    }

    public static int getValueSize(int recordSize, byte[] key) {
        return recordSize - Record.Header.HEADER_SIZE - key.length;
    }

    public static InMemoryIndexMetaData getMetaData(IndexFileEntry entry, int fileId) {
        return new InMemoryIndexMetaData(fileId, Utils.getValueOffset(entry.getRecordOffset(), entry.getKey()), Utils.getValueSize(entry.getRecordSize(), entry.getKey()), entry.getSequenceNumber());
    }

    public static long toUnsignedIntFromInt(int value) {
        return value & 0xffffffffL;
    }

    public static int toSignedIntFromLong(long value) {
        return (int)(value & 0xffffffffL);
    }

    public static int toUnsignedByte(byte value) {
        return value & 0xFF;
    }

    public static byte[] longToBytes(long value) {
        return Longs.toByteArray(value);
    }

    public static long bytesToLong(byte[] bytes) {
        return Longs.fromByteArray(bytes);
    }
}
