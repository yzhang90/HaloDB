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

public class ScaleTest {

    private static final int numberOfRounds = 640_000_000;
    private static final int numberOfThreads = 32;
    private static final int noOfRoundsPerThread = numberOfRounds / numberOfThreads; // 400 million.
    
    private static final int numOfRecords = 100_000_000;
    private String directory = null;
    private static final int seed = 133;
    private static RandomDataGenerator randomDataGenerator = new RandomDataGenerator(seed);

    @Test
    public void testReadWrite() throws Exception {

        String testDir = TestUtils.getTestDirectory("RandomReadWriteTest", "testReadWrite");

        HaloDBStorageEngine dbEngine = createFreshHaloDBStorageEngine(testDir, numOfRecords);
        System.out.println("Opened the database.");

        initDB(dbEngine);
        System.out.println("Initialized database.");

        RWThread[] rws = new RWThread[numberOfThreads];

        long start = System.currentTimeMillis();

        for (int i = 0; i < rws.length; i++) {
            rws[i] = new RWThread(dbEngine, i, noOfRoundsPerThread, numOfRecords);
            rws[i].start();
        }


        for(RWThread rw : rws) {
            try {
                rw.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        long end = System.currentTimeMillis();
        long time = (end - start) / 1000;
        System.out.println("Completed rw in " + time);

        dbEngine.close();

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


    private void initDB(HaloDBStorageEngine db) {
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

            if (i % 1_000_000 == 0) {
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
