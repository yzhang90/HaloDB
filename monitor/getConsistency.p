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
    var snapshotAtGetReq: map[int, (lastSuccessfulPut: tRecord, pendingPuts:set[tRecord])];
    // map from put request id (int) to the expected put request status(i.e, whether the put should succeed).
    var expectedPutReqSuccess: map[int, bool];
    // map from key to the record corresponding to the last successfully responded put for that key
    var putResponseMap: map[string, tRecord];
    // map from a key to the set of concurrent pending put reqs (records) for that key i.e. puts not yet responded.
    var pendingPutReq: map[string, set[tRecord]];
    // map from key to a set of get request id on that key
    var getReqIds: map[string, set[int]];

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
        on ePutReq do AddPutReqToPendingReqSet;
        on ePutResp do UpdatePutResponseMapAndRemoveFromPendingSet;
        on eGetReq do CreateSnapshotForGetReq;
        on eGetResp do CheckGetRespConsistency;
    }

    fun AddPutReqToPendingReqSet(putReq: tPutReq) {
        // add requests in the pendingPutReq set
        var putRecord: tRecord;
        putRecord = putReq.record;

        if(putRecord.key in putResponseMap) {
            if(putRecord.sqr >= putResponseMap[putRecord.key].sqr){
                // the put should fail!
                expectedPutReqSuccess[putReq.rId] = false;
            } else {
                expectedPutReqSuccess[putReq.rId] = true;
            }
        }

        // add to the set of global pending puts
        if(!(putRecord.key in pendingPutReq)){
            pendingPutReq[putRecord.key] = default(set[tRecord]);
        }
        pendingPutReq[putRecord.key] += (putRecord);
        AddConcurrentPutsToGetSnapShot(putRecord);
    }

    fun AddConcurrentPutsToGetSnapShot(putRecord: tRecord) {
        var getReqIdsOnKey: set[int];
        if(putRecord.key in getReqIds) {
            getReqIdsOnKey = getReqIds[putRecord.key];
            for(reqId: int in getReqIdsOnKey) {
                snapshotAtGetReq[reqId].pendingPuts += (putRecord);
            }
        }
    }

    fun UpdatePutResponseMapAndRemoveFromPendingSet(putResp: tPutResp) {
        if(!expectedPutReqSuccess[putResp.rId]){
            assert(!putResp.res, "put is expected to fail but it actually succeed.")
        }
        if(putResp.res){
            // update the latest record value for the key
            putResponseMap[putResp.record.key] = putResp.record;
        }
        // remove the key from pending put request set
        pendingPutReq[putResp.record.key] -= (putResp.record);
    }

    fun CreateSnapshotForGetReq(getReq: tGetReq) {
        var concurrentPuts: set[tRecord];

        if(getReq.key in pendingPutReq)
            concurrentPuts = pendingPutReq[getReq.key];
        else
            concurrentPuts = default(set[tRecord]);

        if(getReq.key in currentDBState){
            snapshotAtGetReq[getReq.rId] = (lastSuccessfulPut = putResponseMap[getReq.key], pendingPuts = concurrentPuts);
        }else{
            snapshotAtGetReq[getReq.rId] = (lastSuccessfulPut = GetEmptyRecord(getReq.key), pendingPuts = concurrentPuts);
        }

        // In the unsync version, we must update snapshotAtGetReq before getReqIds so that in the AddConcurrentPutsToGetSnapShot function, all the request ids in getReqIds appear in snapshotAtGetReq.
        if(!(getReq.key in getReqIds)) {
            getReqIds[getReq.key] = default(set[int]);
        }
        getReqIds[getReq.key] += getReq.rId;
    }

    fun CheckGetRespConsistency (getResp: tGetResp) {
        var getRespRecord: tRecord;
        getRespRecord = getResp.record;

        if(!getResp.res) {
            assert (snapshotAtGetReq[getResp.rId].lastSuccessfulPut == GetEmptyRecord(getRespRecord.key)),
            format ("Get is not Consistent!! Get responded KEYNOTFOUND for a key {0}, even when a record {1} existed", getRespRecord.key, snapshotAtGetReq[getResp.rId].lastSuccessfulPut);
        } else {
            assert snapshotAtGetReq[getResp.rId].lastSuccessfulPut.sqr <= getRespRecord.sqr, format ("For key {0}, expected value of sequencer is >= {1} but got {2}. Get is not Consistent!", getRespRecord.key, snapshotAtGetReq[getResp.rId], getRespRecord.sqr);

            // make sure the response received is a valid response
            assert (snapshotAtGetReq[getResp.rId].lastSuccessfulPut == getRespRecord || getRespRecord in snapshotAtGetReq[getResp.rId].pendingPuts),
                   format ("Incorrect record received in the Get Response!\n Received get response: {0}\n snapshot at get: {1}\n", getRespRecord, snapshotAtGetReq[getResp.rId].lastSuccessfulPut) +
                   format ("pending puts set: {0}\n", snapshotAtGetReq[getResp.rId].pendingPuts);

            // remove the snapshot of the sqr for the key
            snapshotAtGetReq -= (getResp.rId);
            getReqIds[getRespRecord.key] -= (getResp.rId);
        }
    }

}
