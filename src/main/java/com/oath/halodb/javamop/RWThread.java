package com.oath.halodb.javamop;

import com.google.common.primitives.Longs;

import java.lang.*;
import java.io.*;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.Random;

import com.oath.halodb.HaloDBException;

public class RWThread extends Thread {
    private final int tid;
    private final Random rand;
    private final RandomDataGenerator randomDataGenerator;
    private final HaloDBStorageEngine db;
    private final int round;
    private final int numOfRecords;
    private static AtomicInteger counter = new AtomicInteger(1);
    private static AtomicInteger rId = new AtomicInteger(40_000_000);
    public volatile boolean isPause;
    private boolean isPauseOld;

    public RWThread(HaloDBStorageEngine db, int tid, int round, int numOfRecords) {
        this.db = db;
        this.tid = tid;
        this.round = round;
        this.numOfRecords = numOfRecords;
        this.rand = new Random(99 + tid);
        this.randomDataGenerator = new RandomDataGenerator(99 + tid);
        this.isPause = false;
        this.isPauseOld = false;
    }

    @Override
    public void run() {
        for (int i = 0; i < round; i++) {
            try {
                while (isPause) {
                    Thread.sleep(500);
                    if (isPauseOld ==  false) {
                        System.out.println(String.format("Thread#%d is paused.", tid));
                    }
                    isPauseOld = isPause;
                }
            } catch (Exception e) {}

            //System.out.println(String.format("Thread#%d is running.", tid));
            isPauseOld = isPause;
            int operation = rand.nextInt(2);
            long id = (long)rand.nextInt(numOfRecords);
            //int sleep1 = rand.nextInt(500);
            //int sleep2 = rand.nextInt(500);
            //int sleep3 = rand.nextInt(500);
            int resId = rId.getAndIncrement();
            if(resId % 1_000_000 == 0) {
                System.out.printf("Processed %d requests\n", resId);
            }
            if (operation == 0) {
                // read
                byte[] key = longToBytes(id);
                try {
                    //byte[] result = db.get(key, resId, sleep1, sleep2);
                    byte[] result = db.get(key, resId, 0, 0);
                    if (result == null) {
                        System.out.println("[HaloRead#" + tid + "#" + i + "#" + resId + "] " + "No value for key " +id);
                    } else {
                        //System.out.println("[HaloRead#" + tid + "#" + i + "#" + resId + "] " + id + "," + bytesToLong(result));
                    }
                } catch (HaloDBException e) {}
            } else {
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
              
                //db.put(key, value, resId, sleep1, sleep2, sleep3);
                db.put(key, value, resId, 0, 0, 0);
                //System.out.println("[HaloWrite#" + tid + "#" + i + "#" + resId + "] " + id + ",");
            }
        }
    }

    public void pauseExec() {
        isPause = true;
    }

    public void resumeExec() {
        isPause = false;
    }

    static byte[] longToBytes(long value) {
        return Longs.toByteArray(value);
    }

    static long bytesToLong(byte[] bytes) {
        return Longs.fromByteArray(bytes);
    }

}
