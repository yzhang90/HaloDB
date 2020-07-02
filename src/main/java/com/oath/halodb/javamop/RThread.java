package com.oath.halodb.javamop;

import com.google.common.primitives.Longs;

import java.lang.*;
import java.io.*;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.Random;

import com.oath.halodb.HaloDBException;

public class RThread extends Thread {
    private final int tid;
    private final HaloDBStorageEngine db;
    private final Random rand;
    private final int round;
    private final int numOfRecords;
    private static AtomicInteger counter = new AtomicInteger(1);
    private static AtomicInteger rId = new AtomicInteger(40_000_000);

    public RThread(HaloDBStorageEngine db, int tid, int round, int numOfRecords) {
        this.db = db;
        this.tid = tid;
        this.round = round;
        this.numOfRecords = numOfRecords;
        this.rand = new Random(99 + tid);
    }

    @Override
    public void run() {
        for (int i = 0; i < round; i++) {
            long id = (long)rand.nextInt(numOfRecords);
            int resId = rId.getAndIncrement();
            if(resId % 10_000_000 == 0) {
                System.out.printf("Processed %d requests\n", resId);
            }

            byte[] key = longToBytes(id);
            try {
                byte[] result = db.get(key, resId, 0, 0);
                if (result == null) {
                    System.out.println("[HaloRead#" + tid + "#" + i + "#" + resId + "] " + "No value for key " +id);
                }
            } catch (HaloDBException e) {}
        }
    }

    static byte[] longToBytes(long value) {
        return Longs.toByteArray(value);
    }

    static long bytesToLong(byte[] bytes) {
        return Longs.fromByteArray(bytes);
    }


}

