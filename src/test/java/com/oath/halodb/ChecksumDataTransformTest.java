package com.oath.halodb;

import com.oath.halodb.javamop.SimpleDataTransformer;
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
        Record.Header header = new Record.Header( crc32.getValue()
                                                , version , (byte)key.length, value.length, sequenceNumber);
        record.setHeader(header);
        return record;
    }

    private static long calculateChecksum(Record record) {
        ByteBuffer[] buffers = record.serialize();
        CRC32 crc32 = new CRC32();
        crc32.update(buffers[0].array(), Record.Header.VERSION_OFFSET, buffers[0].array().length - Record.Header.CHECKSUM_SIZE);
        crc32.update(record.getKey());
        crc32.update(record.getValue());
        return crc32.getValue();
    }

    @Test
    public void testSimpleTransform() {
        Record testRecord = generateRandomRecord();
        try {
            Record outRecord = SimpleDataTransformer.simpleTransform(testRecord);
            Assert.assertEquals(outRecord.getHeader().getCheckSum(), calculateChecksum(outRecord));
        }
        catch(HaloDBException e) {
             Assert.fail(e.getMessage());
        }
    }
}


