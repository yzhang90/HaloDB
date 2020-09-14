event monitor_Init: seq[tRecord];
event eBrickGetReq: tBrickGetReq;
event eBrickGetResp: tBrickGetResp;
event eBrickPutReq: tBrickPutReq;
event eBrickPutResp: tBrickPutResp;
// client is request sender, rid is the unique request Id
type tBrickGetReq = (client:machine, key:string, rId: int);
// res is true if the key is found and false if not
type tBrickGetResp = (res: bool, record: tRecord, rId: int);
type tBrickPutReq = (client:machine, record: tRecord, rId: int);
type tBrickPutResp = (res: bool, record: tRecord, rId: int);
/*sqr is a monotonically increasing time stamp value*/
type tRecord = (key: string, val: int, sqr: int);


spec GetConsistency observes monitor_Init, eGetResp, eGetReq, ePutResp, ePutReq {
    // map from get request id (int) to the snapshot of the record when the get request was issued.
    var snapshotAtGetReq: map[int, tRecord];
    // map from put request id (int) to the expected put request status(i.e, whether the put should succeed).
    var expectedPutReqSuccess: map[int, bool];
    // map from key to the record corresponding to the last successfully responded put for that key
    var putResponseMap: map[string, tRecord];

    start state Init {
        on monitor_Init goto WaitForGetAndPutOperations with (initKeys: seq[tRecord]) {
            var i : int;
            // add all the existing keys to the put responses map i.e. get the initial snapshot of the system
            while(i<sizeof(initKeys)){
                putResponseMap[initKeys[i].key] = initKeys[i];
                i = i + 1;
            }
        }
    }

    state WaitForGetAndPutOperations {
        on ePutReq do AddPutReqExpectedResponse;
        on ePutResp do UpdatePutResponseMap;
        on eGetReq do CreateSnapshotForGetReq;
        on eGetResp do CheckGetRespConsistency;
    }

    fun AddPutReqExpectedResponse(putReq: tPutReq) {
        // add requests in the pendingPutReq set
        var putRecord: tRecord;
        putRecord = putReq.record;

        if(putRecord.key in putResponseMap) {
            if(putRecord.sqr <= putResponseMap[putRecord.key].sqr){
                // the put should fail!
                expectedPutReqSuccess[putReq.rId] = false;
            } else {
                expectedPutReqSuccess[putReq.rId] = true;
            }
        } else {
            expectedPutReqSuccess[putReq.rId] = true;
        }
    }

    fun UpdatePutResponseMap(putResp: tPutResp) {
        if(!expectedPutReqSuccess[putResp.rId]){
            assert(!putResp.res, "put is expected to fail but it actually succeed.")
        }
        if(putResp.res){
            // update the latest record value for the key
            putResponseMap[putResp.record.key] = putResp.record;
        }
    }

    fun CreateSnapshotForGetReq(getReq: tGetReq) {
        if(getReq.key in putResponseMap){
            snapshotAtGetReq[getReq.rId] = putResponseMap[getReq.key];
        }else{
            snapshotAtGetReq[getReq.rId] = GetEmptyRecord(getReq.key);
        }
    }

    fun CheckGetRespConsistency (getResp: tGetResp) {
        var getRespRecord: tRecord;
        getRespRecord = getResp.record;

        if(!getResp.res) {
            assert (snapshotAtGetReq[getResp.rId] == GetEmptyRecord(getRespRecord.key)),
            format ("Get is not Consistent!! Get responded KEYNOTFOUND for a key {0}, even when a record {1} existed", getRespRecord.key, snapshotAtGetReq[getResp.rId]);
        } else {
            assert snapshotAtGetReq[getResp.rId].sqr <= getRespRecord.sqr, format ("For key {0}, expected value of sequencer is >= {1} but got {2}. Get is not Consistent!", getRespRecord.key, snapshotAtGetReq[getResp.rId].sqr, getRespRecord.sqr);
            // remove the snapshot of the sqr for the key
            snapshotAtGetReq -= (getResp.rId);
        }
    }
}
