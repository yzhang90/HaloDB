package mop;

import com.google.common.primitives.Longs;
import java.util.zip.CRC32;

class MopUtils {

    static byte[] longToBytes(long value) {
        return Longs.toByteArray(value);
    }

    static long bytesToLong(byte[] bytes) {
        return Longs.fromByteArray(bytes);
    }

    static long computeChecksum(byte[] input) {
        CRC32 crc32 = new CRC32();
        crc32.update(input);
        return crc32.getValue();
    }
}
