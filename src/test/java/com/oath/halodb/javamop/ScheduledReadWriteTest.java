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

public class ScheduledReadWriteTest {

    private static final int numOfRecords = 10;
    private static AtomicInteger rId = new AtomicInteger(1);
    private String directory = null;

    @Test
    public void testReadWrite() throws Exception {
        String testDir = TestUtils.getTestDirectory("RandomReadWriteTest", "testReadWrite");

        HaloDBStorageEngine dbEngine = getTestHaloDBStorageEngine(testDir, numOfRecords);

        WThread t1 = new WThread(dbEngine, 1);
        WRThread t2 = new WRThread(dbEngine, 2);

        t1.start();
        t2.start();

        t1.join();
        t2.join();

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

    class WThread extends Thread {
        private final int tid;
        private final HaloDBStorageEngine db;


        WThread(HaloDBStorageEngine db, int tid) {
            this.db = db;
            this.tid = tid;
        }

        @Override
        public void run() {
            byte[] key = longToBytes(0);
            byte[] value = longToBytes(1);
            int resId = rId.getAndIncrement(); 
            db.put(key, value, resId, 500, 0);
            System.out.println(String.format("[HaloWrite#%d#%d] 0, 1", this.tid, resId));
        }
    }

    class WRThread extends Thread {
        private final int tid;
        private final HaloDBStorageEngine db;


        WRThread(HaloDBStorageEngine db, int tid) {
            this.db = db;
            this.tid = tid;
        }

        @Override
        public void run() {
            byte[] key = longToBytes(0);
            byte[] value = longToBytes(2);
            int resId = rId.getAndIncrement();
            db.put(key, value, resId, 0, 0);
            System.out.println(String.format("[HaloWrite#%d#%d] 0, 2", this.tid, resId));
            resId = rId.getAndIncrement();
            try {
                byte[] result = db.get(key, resId, 1000, 0);
                System.out.println(String.format("[HaloRead#%d#%d] 0, %d", this.tid, resId, bytesToLong(result)));
            } catch (Exception e) {}
        }
    }

}