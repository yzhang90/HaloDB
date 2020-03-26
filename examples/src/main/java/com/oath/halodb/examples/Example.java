package com.oath.halodb.examples;

import com.google.common.primitives.Longs;

import java.io.File;
import java.lang.*;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.Map;
import java.util.HashMap;
import java.util.Random;

public class Example {

    private static final int round = 20;
    private static final int numberOfRecords = 10;
    private static AtomicInteger counter = new AtomicInteger();
    private static AtomicInteger rId = new AtomicInteger(1);


    public static void main(String[] args) throws Exception {
        String directoryName = args[0];

        File dir = new File(directoryName);
        final HaloDBStorageEngine db = new HaloDBStorageEngine(dir, numberOfRecords);
        db.open();
        System.out.println("Opened the database.");

        RWThread t1 = new RWThread(db, 1);
        RWThread t2 = new RWThread(db, 2);
        RWThread t3 = new RWThread(db, 3);

        t1.start();
        t2.start();
        t3.start();

        t1.join();
        t2.join();
        t3.join();

        db.close();
    }

    public static byte[] longToBytes(long value) {
        return Longs.toByteArray(value);
    }

    public static long bytesToLong(byte[] bytes) {
        return Longs.fromByteArray(bytes);
    }

    static class RWThread extends Thread {
        private final int tid;
        private final Random rand;
        private final HaloDBStorageEngine db;


        RWThread(HaloDBStorageEngine db, int tid) {
            this.db = db;
            this.tid = tid;
            this.rand = new Random(99 + tid);
        }

        @Override
        public void run() {
            for (int i = 0; i < round; i++) {
                int operation = rand.nextInt(2);
                int resId = rId.getAndIncrement();
                if (operation == 0) {
                    // read
                    long id = (long)rand.nextInt(numberOfRecords);
                    byte[] key = longToBytes(id);
                    byte[] result = db.get(key, resId);

                    if (result == null) {
                        System.out.println("[HaloRead#" + tid + "#" + i + "#" + resId + "] " + "No value for key " +id);
                    } else {
                        System.out.println("[HaloRead#" + tid + "#" + i + "#" + resId + "] " + id + "," + bytesToLong(result));
                    }

                } else {
                    // write
                    long id = (long)rand.nextInt(numberOfRecords);
                    byte[] key = longToBytes(id);
                    long val = (long)counter.getAndIncrement();
                    byte[] value = longToBytes(val);

                    db.put(key, value, resId);
                    System.out.println("[HaloWrite#" + tid + "#" + i + "#" + resId + "] " + id + "," + val);
                }
            }
        }
    }

}
