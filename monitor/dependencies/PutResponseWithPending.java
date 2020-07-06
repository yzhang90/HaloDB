package mop;

import java.util.Set;

public final class PutResponseWithPending {
    public MonitorRecord lastSuccessfulPut;
    public Set<MonitorRecord> pendingRecords;

    public PutResponseWithPending(MonitorRecord lsp, Set<MonitorRecord> prs) {
        this.lastSuccessfulPut = lsp;
        this.pendingRecords = prs;
    }

}
