package com.oath.halodb.javamop;

import com.oath.halodb.HaloDB;
import com.oath.halodb.HaloDBException;
import com.oath.halodb.Record;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.zip.CRC32;

public class SimpleDataTransformer {

    static byte[] dataComputation(byte[] in) {
        byte[] out = in.clone();
        if(!(in.length > 0)) {
            return out;
        }
        out[0] = (byte) (in[0] + 1);
        return out;
    }

    static long calculateChecksum(Record record) {
        ByteBuffer[] buffers = record.serialize();
        CRC32 crc32 = new CRC32();
        crc32.update(buffers[0].array(), Record.Header.VERSION_OFFSET, buffers[0].array().length - Record.Header.CHECKSUM_SIZE);
        crc32.update(record.getKey());
        crc32.update(record.getValue());
        return crc32.getValue();
    }

    public static Record simpleTransform(Record in) throws HaloDBException{
        if(!in.verifyChecksum())
            throw new HaloDBException("Initial Checksum Verification Failed");

        byte[] key = in.getKey();
        byte[] value = in.getValue();
        byte[] newValue = dataComputation(value);
        long sequenceNumber = in.getSequenceNumber();
        int version = in.getVersion();
        Record out = new Record(key, newValue);
        out.setSequenceNumber(sequenceNumber);
        out.setVersion(version);

        long checksum = calculateChecksum(out);

        Record.Header header = new Record.Header(checksum, version, (byte)key.length, value.length, sequenceNumber);
        out.setHeader(header);

        byte[] redundancyValue = dataComputation(value);

        if(!(Arrays.equals(redundancyValue, newValue)))
            throw new HaloDBException("Redundancy Checks Failed (Values Mismatch)");

        Record redundancyRecord = new Record(key, redundancyValue);
        redundancyRecord.setSequenceNumber(sequenceNumber);
        redundancyRecord.setVersion(version);

        long redundancyChecksum = calculateChecksum(redundancyRecord);

        if (!(redundancyChecksum == checksum))
            throw new HaloDBException("Redundancy Checks Failed (Checksums)");

        return out;
    }
}
