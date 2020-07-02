package com.oath.halodb.javamop;

import com.google.common.primitives.Longs;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.io.ByteArrayOutputStream;
import java.lang.*;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.Map;
import java.util.HashMap;
import java.util.Random;

import com.oath.halodb.HaloDBException;
import com.oath.halodb.TestUtils;

public class RandomReadWriteTest {

    private static final int round = 50_000_000;
    private static final int numOfRecords = 1_000_000;

    private String directory = null;

    private static final int seed = 133;
    private static RandomDataGenerator randomDataGenerator = new RandomDataGenerator(seed);

    @Test
    public void testReadWrite() throws Exception {
        Runtime rt = Runtime.getRuntime();

        long memoryBefore = rt.totalMemory() - rt.freeMemory();

        String testDir = TestUtils.getTestDirectory("RandomReadWriteTest", "testReadWrite");

        HaloDBStorageEngine dbEngine = createFreshHaloDBStorageEngine(testDir, numOfRecords);

        System.out.println("Opened the database.");
        
        initData(dbEngine);

        System.out.println("Initialized database.");

        long start = System.currentTimeMillis();

        RWThread t1 = new RWThread(dbEngine, 1, round, numOfRecords);
        RWThread t2 = new RWThread(dbEngine, 2, round, numOfRecords);
        //RWThread t3 = new RWThread(dbEngine, 3, round, numOfRecords);

        t1.start();
        t2.start();
        //t3.start();

        /*Thread.sleep(2000);
        t1.pauseExec();
        t2.pauseExec();
        t3.pauseExec();

        Thread.sleep(10000);
        t1.resumeExec();
        t2.resumeExec();
        t3.resumeExec();

        dummy();*/

        t1.join();
        t2.join();
        //t3.join();

        long end = System.currentTimeMillis();
        long time = (end - start) / 1000;
        System.out.println("Completed rw in " + time);

        long memoryAfter = rt.totalMemory() - rt.freeMemory();
        double memory = (memoryAfter - memoryBefore) / (1024*1024);
        System.out.printf("Memory usage: %f mb\n", memory);

        dbEngine.close();

    }

    void dummy() {
        try {
            Thread.sleep(2000);
        } catch (Exception e) {}
        return;
    }

    HaloDBStorageEngine createFreshHaloDBStorageEngine(String directory, int numOfRecords) throws HaloDBException {
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

    private void initData(HaloDBStorageEngine db) {
        Random rand = new Random(137);
        for(int i = 0; i < numOfRecords; i++) {
            byte[] key = longToBytes(i);
            byte[] seqBytes = longToBytes(0);
            byte[] payload = randomDataGenerator.getData(1024);
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            try {
                outputStream.write(payload);
                outputStream.write(seqBytes);
            } catch (Exception e) {}
            byte[] value = outputStream.toByteArray();

            db.put(key, value, i, 0, 0, 0);

            if (i % 100_000 == 0) {
                System.out.printf("Wrote %d records\n", i);
            }
        }

    }

    static byte[] longToBytes(long value) {
        return Longs.toByteArray(value);
    }

    static long bytesToLong(byte[] bytes) {
        return Longs.fromByteArray(bytes);
    }

}
