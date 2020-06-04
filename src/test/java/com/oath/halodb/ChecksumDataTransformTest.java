package com.oath.halodb;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.util.Random;
import java.util.zip.CRC32;

public class ChecksumDataTransformTest {

    static Record generateRandomRecord() {
        byte[] key = TestUtils.generateRandomByteArray();
        byte[] value = TestUtils.generateRandomByteArray();
        Random random = new Random();
        long sequenceNumber = random.nextLong();
        int version = 13;


        Record record = new Record(key, value);
        record.setSequenceNumber(sequenceNumber);
        record.setVersion(version);

        ByteBuffer[] buffers = record.serialize();
        CRC32 crc32 = new CRC32();
        crc32.update(buffers[0].array(), Record.Header.VERSION_OFFSET, buffers[0].array().length - Record.Header.CHECKSUM_SIZE);
        crc32.update(key);
        crc32.update(value);

        return record;
    }

    static boolean regenrateHeader(Record record) {
        byte[] key = record.getKey();
        byte[] value = record.getValue();
        int version = record.getVersion();
        long sequenceNumber = record.getSequenceNumber();

        // regenerate Header
        ByteBuffer[] buffers = record.serialize();
        CRC32 crc32 = new CRC32();
        crc32.update(buffers[0].array(), Record.Header.VERSION_OFFSET, buffers[0].array().length - Record.Header.CHECKSUM_SIZE);
        crc32.update(key);
        crc32.update(value);
        long checksum = putInt(Record.Header.CHECKSUM_OFFSET, Utils.toSignedIntFromLong(crc32.getValue()));
        return
    }

    @Test
    public void testChecksumVerification() {
        Record testRecord = generateRandomRecord();
        Record.Header header =
    }

}
