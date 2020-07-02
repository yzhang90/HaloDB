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

public class RandomReadTest {

    private static final int round = 100_000_000;
    private static final int numOfRecords = 1_000_000;

    private String directory = null;

    private static final int seed = 133;
    private static RandomDataGenerator randomDataGenerator = new RandomDataGenerator(seed);

    @Test
    public void testReadWrite() throws Exception {

        Runtime rt = Runtime.getRuntime();

        long memoryBefore = rt.totalMemory() - rt.freeMemory();

        String testDir = TestUtils.getTestDirectory("RandomReadTest", "testRead");

        HaloDBStorageEngine dbEngine = createFreshHaloDBStorageEngine(testDir, numOfRecords);

        System.out.println("Opened the database.");
        
        initData(dbEngine);

        System.out.println("Initialized database.");

        dummy();

        long start = System.currentTimeMillis();

        RThread t1 = new RThread(dbEngine, 1, round, numOfRecords);
        RThread t2 = new RThread(dbEngine, 2, round, numOfRecords);

        t1.start();
        t2.start();

        t1.join();
        t2.join();

        long end = System.currentTimeMillis();
        long time = (end - start) / 1000;
        System.out.println("Completed r in " + time);

        long memoryAfter = rt.totalMemory() - rt.freeMemory();
        double memory = (memoryAfter - memoryBefore) / (1024*1024);
        System.out.printf("Memory usage: %f mb\n", memory);


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

    private void initData(HaloDBStorageEngine db) {
        Random rand = new Random(137);
        for(int i = 0; i < numOfRecords; i++) {
            byte[] key = longToBytes(i);
            byte[] seqBytes = longToBytes(0);
            byte[] payload = randomDataGenerator.getData(1024);
            rand.nextBytes(payload);
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream( );
            try {
                outputStream.write(payload);
                outputStream.write(seqBytes);
            } catch (Exception e) {}
            byte[] value = outputStream.toByteArray();

            db.put(key, value, i, 0, 0, 0);

            /*if (i % 10_000 == 0) {
                System.out.printf("Wrote %d records\n", i);
            }*/
        }

    }

    private void dummy() {

    }

    static byte[] longToBytes(long value) {
        return Longs.toByteArray(value);
    }

    static long bytesToLong(byte[] bytes) {
        return Longs.fromByteArray(bytes);
    }

}
