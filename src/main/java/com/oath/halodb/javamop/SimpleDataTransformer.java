package com.oath.halodb.javamop;

import com.oath.halodb.Record;
import com.oath.halodb.Utils;

import java.nio.ByteBuffer;
import java.util.Random;
import java.util.zip.CRC32;

public class SimpleDataTransformer {

    static byte[] simpleTransform(byte[] in) {
        if(!(in.length > 0)) {
            return in;
        }
        in[0] = (byte) (in[0] + 1);
        return in;
    }

    private long getChecksum(Record record) {
        return Record.Header.deserialize(record.serialize()[0]).getCheckSum();
    }

    public static Record simpleTransform(Record in) {

        byte[] key = in.getKey();
        byte[] value = in.getValue();
        byte[] newValue = simpleTransform(value);
        long sequenceNumber = in.getSequenceNumber();
        int version = in.getVersion();
        Record out = new Record(key, newValue);
        out.setSequenceNumber(sequenceNumber);
        out.setVersion(version);

        ByteBuffer[] buffers = out.serialize();
        CRC32 crc32 = new CRC32();
        crc32.update(buffers[0].array(), Record.Header.VERSION_OFFSET, buffers[0].array().length - Record.Header.CHECKSUM_SIZE);
        crc32.update(key);
        crc32.update(newValue);

        long checksum = crc32.getValue();

        Record.Header header = new Record.Header(checksum, version, (byte)key.length, value.length, sequenceNumber);
        out.setHeader(header);
        return out;
    }
}
