package com.oath.halodb;

public class DBPutResult {

    public boolean success;
    public final byte[] key, value;

    public DBPutResult(boolean success, byte[] key, byte[] value) {
        this.success = success;
        this.key = key;
        this.value = value;
    }
    
}

