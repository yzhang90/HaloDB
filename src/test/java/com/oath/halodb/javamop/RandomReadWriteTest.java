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

    private static final int round = 10000;
    private static final int numOfRecords = 10;
    private String directory = null;

    @Test
    public void testReadWrite() throws Exception {
        String testDir = TestUtils.getTestDirectory("RandomReadWriteTest", "testReadWrite");

        HaloDBStorageEngine dbEngine = createFreshHaloDBStorageEngine(testDir, numOfRecords);

        System.out.println("Opened the database.");

        RWThread t1 = new RWThread(dbEngine, 1, round, numOfRecords);
        RWThread t2 = new RWThread(dbEngine, 2, round, numOfRecords);
        RWThread t3 = new RWThread(dbEngine, 3, round, numOfRecords);

        t1.start();
        t2.start();
        t3.start();

        Thread.sleep(2000);
        /*t1.pauseExec();
        t2.pauseExec();
        t3.pauseExec();

        Thread.sleep(10000);
        t1.resumeExec();
        t2.resumeExec();
        t3.resumeExec();*/

        dummy();

        t1.join();
        t2.join();
        t3.join();

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

    static byte[] longToBytes(long value) {
        return Longs.toByteArray(value);
    }

    static long bytesToLong(byte[] bytes) {
        return Longs.fromByteArray(bytes);
    }

}
