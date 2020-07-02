package mop;

public final class MapRecord {
    public final long key;
    public final long checksum;
    public final long seq;

    public MapRecord(long key, long checksum, long seq) {
        this.key = key;
        this.checksum = checksum;
        this.seq = seq;
    }

}
