package com.oath.halodb.javamop;

import com.google.common.primitives.Longs;

import java.lang.*;
import java.io.*;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.Random;

import com.oath.halodb.HaloDBException;

public class WThread extends Thread {
    private final int tid;
    private final Random rand;
    private final RandomDataGenerator randomDataGenerator;
    private final HaloDBStorageEngine db;
    private final int round;
    private final int numOfRecords;
    private static AtomicInteger counter = new AtomicInteger(1);
    private static AtomicInteger rId = new AtomicInteger(40_000_000);

    public WThread(HaloDBStorageEngine db, int tid, int round, int numOfRecords) {
        this.db = db;
        this.tid = tid;
        this.round = round;
        this.numOfRecords = numOfRecords;
        this.rand = new Random(99 + tid);
        this.randomDataGenerator = new RandomDataGenerator(99 + tid);
    }

    @Override
    public void run() {
        for (int i = 0; i < round; i++) {
            long id = (long)rand.nextInt(numOfRecords);
            int resId = rId.getAndIncrement();
            if(resId % 1_000_000 == 0) {
                System.out.printf("Processed %d requests\n", resId);
            }
            // write
            byte[] key = longToBytes(id);
            long seq = (long)counter.getAndIncrement();
            byte[] seqBytes = longToBytes(seq);
            byte[] payload = randomDataGenerator.getData(1024);
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream( );
            try {
                outputStream.write(payload);
                outputStream.write(seqBytes);
            } catch (Exception e) {}
            byte[] value = outputStream.toByteArray();
              
            db.put(key);
        }
    }

    static byte[] longToBytes(long value) {
        return Longs.toByteArray(value);
    }

    static long bytesToLong(byte[] bytes) {
        return Longs.fromByteArray(bytes);
    }

}

