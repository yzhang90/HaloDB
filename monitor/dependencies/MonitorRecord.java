package mop;

public final class MonitorRecord {
    public final long key;
    public final long checksum;
    public final long seq;

    public MonitorRecord(long key, long checksum, long seq) {
        this.key = key;
        this.checksum = checksum;
        this.seq = seq;
    }


    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (new Long(key)).hashCode();
        result = prime * result + (new Long(checksum)).hashCode();
        result = prime * result + (new Long(seq)).hashCode();
        return result;
    }


    @Override
    public boolean equals(Object obj){
        if (obj == this) { 
            return true; 
        } 

        if (obj instanceof MonitorRecord) {
            MonitorRecord mr = (MonitorRecord) obj;
            return (mr.key == this.key && mr.checksum == this.checksum && mr.seq == this.seq);
        } else {
            return false;
        }
    }

}
