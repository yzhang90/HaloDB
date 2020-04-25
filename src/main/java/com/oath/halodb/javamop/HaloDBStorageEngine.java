package com.oath.halodb.javamop;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.oath.halodb.DBPutResult;
import com.oath.halodb.HaloDB;
import com.oath.halodb.HaloDBException;
import com.oath.halodb.HaloDBOptions;

import java.io.File;
import java.util.Random;

public class HaloDBStorageEngine {

    private final File dbDirectory;

    public HaloDB db;
    private final long noOfRecords;

    public HaloDBStorageEngine(File dbDirectory, long noOfRecords) {
        this.dbDirectory = dbDirectory;
        this.noOfRecords = noOfRecords;
    }

    public DBPutResult put(byte[] key, byte[] value, int rId, int sleep1, int sleep2) {
        try {
            System.out.println(String.format("Sleep %d millis before put.", sleep1));
            Thread.sleep(sleep1);
            DBPutResult result = db.put(key, value);
            System.out.println(String.format("Sleep %d millis after put.", sleep2));
            Thread.sleep(sleep2);
            return result;
        } catch (Exception e) {
            e.printStackTrace();
        }

        return new DBPutResult(false, null, null);
    }

    public byte[] get(byte[] key, int rId, int sleep1, int sleep2) throws HaloDBException {
        try {
            System.out.println(String.format("Sleep %d millis before get.", sleep1));
            Thread.sleep(sleep1);
            byte[] result = db.get(key);
            System.out.println(String.format("Sleep %d millis after get.", sleep2));
            Thread.sleep(sleep2);
            return result;
        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }

    public void delete(byte[] key) {
        try {
            db.delete(key);
        } catch (HaloDBException e) {
            e.printStackTrace();
        }
    }

    public void open() {
        HaloDBOptions opts = new HaloDBOptions();
        opts.setMaxFileSize(1024*1024*1024);
        opts.setCompactionThresholdPerFile(0.50);
        opts.setFlushDataSizeBytes(10 * 1024 * 1024);
        opts.setNumberOfRecords(Ints.checkedCast(2 * noOfRecords));
        opts.setCompactionJobRate(135 * 1024 * 1024);
        opts.setUseMemoryPool(true);
        opts.setFixedKeySize(8);

        try {
            db = HaloDB.open(dbDirectory, opts);
        } catch (HaloDBException e) {
            e.printStackTrace();
        }
    }

    public void close() {
        if (db != null){
            try {
                db.close();
            } catch (HaloDBException e) {
                e.printStackTrace();
            }
        }

    }

    public long size() {
        return db.size();
    }

    public void printStats() {
        
    }

    public String stats() {
        return db.stats().toString();
    }
}
