package com.oath.halodb.javamop;

import com.google.common.primitives.Longs;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.lang.*;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.Map;
import java.util.HashMap;
import java.util.Random;

import com.oath.halodb.HaloDBException;
import com.oath.halodb.TestUtils;

public class RandomReadWriteTest {

    private static final int round = 20;
    private static final int numOfRecords = 10;
    private static AtomicInteger counter = new AtomicInteger();
    private static AtomicInteger rId = new AtomicInteger(1);
    private String directory = null;

    @Test
    public void testReadWrite() throws Exception {
        String testDir = TestUtils.getTestDirectory("RandomReadWriteTest", "testReadWrite");

        HaloDBStorageEngine dbEngine = getTestHaloDBStorageEngine(testDir, numOfRecords);

        System.out.println("Opened the database.");

        RWThread t1 = new RWThread(dbEngine, 1);
        RWThread t2 = new RWThread(dbEngine, 2);
        RWThread t3 = new RWThread(dbEngine, 3);

        t1.start();
        t2.start();
        t3.start();

        t1.join();
        t2.join();
        t3.join();

        dbEngine.close();
    }

    HaloDBStorageEngine getTestHaloDBStorageEngine(String directory, int numOfRecords) throws HaloDBException {
        this.directory = directory;
        File dir = new File(directory);
        try {
            TestUtils.deleteDirectory(dir);
        } catch (IOException e) {
            throw new HaloDBException(e);
        }
        HaloDBStorageEngine dbEngine = new HaloDBStorageEngine(dir, numOfRecords);
        dbEngine.open();

        return dbEngine;
    }

    static byte[] longToBytes(long value) {
        return Longs.toByteArray(value);
    }

    static long bytesToLong(byte[] bytes) {
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
                    long id = (long)rand.nextInt(numOfRecords);
                    byte[] key = longToBytes(id);
                    try {
                        byte[] result = db.get(key, resId);
                        if (result == null) {
                            System.out.println("[HaloRead#" + tid + "#" + i + "#" + resId + "] " + "No value for key " +id);
                        } else {
                            System.out.println("[HaloRead#" + tid + "#" + i + "#" + resId + "] " + id + "," + bytesToLong(result));
                        }
                    } catch (HaloDBException e) {}
                } else {
                    // write
                    long id = (long)rand.nextInt(numOfRecords);
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
