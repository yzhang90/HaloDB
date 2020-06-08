package mop;
import java.util.*;
import java.io.*;
import com.oath.halodb.*;
import com.oath.halodb.javamop.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.*;

import java.lang.ref.*;
import org.aspectj.lang.*;

aspect BaseAspect {
	pointcut notwithin() :
	!within(sun..*) &&
	!within(java..*) &&
	!within(javax..*) &&
	!within(com.sun..*) &&
	!within(org.dacapo.harness..*) &&
	!within(org.apache.commons..*) &&
	!within(org.apache.geronimo..*) &&
	!within(net.sf.cglib..*) &&
	!within(mop..*) &&
	!within(javamoprt..*) &&
	!within(rvmonitorrt..*) &&
	!within(com.runtimeverification..*);
}

public aspect checkKeyValueConsistencyMonitorAspect implements com.runtimeverification.rvmonitor.java.rt.RVMObject {
	public checkKeyValueConsistencyMonitorAspect(){
	}

	// Declarations for the Lock
	static ReentrantLock checkKeyValueConsistency_MOPLock = new ReentrantLock();
	static Condition checkKeyValueConsistency_MOPLock_cond = checkKeyValueConsistency_MOPLock.newCondition();

	pointcut MOP_CommonPointCut() : !within(com.runtimeverification.rvmonitor.java.rt.RVMObject+) && !adviceexecution() && BaseAspect.notwithin();
	pointcut checkKeyValueConsistency_getReqExec(byte[] key, int rId) : (execution(byte[] HaloDBStorageEngine.get(byte[], int)) && args(key, rId)) && MOP_CommonPointCut();
	before (byte[] key, int rId) : checkKeyValueConsistency_getReqExec(key, rId) {
		checkKeyValueConsistencyRuntimeMonitor.getReqExecEvent(key, rId);
	}

	pointcut checkKeyValueConsistency_getReqCall(byte[] key, int rId) : (call(byte[] HaloDBStorageEngine.get(byte[], int)) && args(key, rId)) && MOP_CommonPointCut();
	before (byte[] key, int rId) : checkKeyValueConsistency_getReqCall(key, rId) {
		checkKeyValueConsistencyRuntimeMonitor.getReqCallEvent(key, rId);
	}

	pointcut checkKeyValueConsistency_putReqExec(byte[] key, byte[] value, int rId) : (execution(* HaloDBStorageEngine.put(byte[], byte[], int)) && args(key, value, rId)) && MOP_CommonPointCut();
	before (byte[] key, byte[] value, int rId) : checkKeyValueConsistency_putReqExec(key, value, rId) {
		checkKeyValueConsistencyRuntimeMonitor.putReqExecEvent(key, value, rId);
	}

	pointcut checkKeyValueConsistency_putReqCall(byte[] key, byte[] value, int rId) : (call(* HaloDBStorageEngine.put(byte[], byte[], int)) && args(key, value, rId)) && MOP_CommonPointCut();
	before (byte[] key, byte[] value, int rId) : checkKeyValueConsistency_putReqCall(key, value, rId) {
		checkKeyValueConsistencyRuntimeMonitor.putReqCallEvent(key, value, rId);
	}

	pointcut checkKeyValueConsistency_putResExec() : (execution(DBPutResult HaloDBStorageEngine.put(byte[], byte[], int))) && MOP_CommonPointCut();
	after () returning (DBPutResult result) : checkKeyValueConsistency_putResExec() {
		checkKeyValueConsistencyRuntimeMonitor.putResExecEvent(result);
	}

	pointcut checkKeyValueConsistency_putResCall(byte[] key, byte[] value, int rId) : (call(DBPutResult HaloDBStorageEngine.put(byte[], byte[], int)) && args(key, value, rId)) && MOP_CommonPointCut();
	after (byte[] key, byte[] value, int rId) returning (DBPutResult result) : checkKeyValueConsistency_putResCall(key, value, rId) {
		checkKeyValueConsistencyRuntimeMonitor.putResCallEvent(key, value, rId, result);
	}

	pointcut checkKeyValueConsistency_getResExec() : (execution(byte[] HaloDBStorageEngine.get(byte[], int))) && MOP_CommonPointCut();
	after () returning (byte[] result) : checkKeyValueConsistency_getResExec() {
		checkKeyValueConsistencyRuntimeMonitor.getResExecEvent(result);
	}

	after (byte[] key, int rId) returning (byte[] result) : checkKeyValueConsistency_getReqCall(key, rId) {
		checkKeyValueConsistencyRuntimeMonitor.getResCallEvent(key, rId, result);
	}

	static HashMap<Thread, Runnable> checkKeyValueConsistency_monitor_init_ThreadToRunnable = new HashMap<Thread, Runnable>();
	static Thread checkKeyValueConsistency_monitor_init_MainThread = null;

	after (Runnable r) returning (Thread t): ((call(Thread+.new(Runnable+,..)) && args(r,..))|| (initialization(Thread+.new(ThreadGroup+, Runnable+,..)) && args(ThreadGroup, r,..))) && MOP_CommonPointCut() {
		while (!checkKeyValueConsistency_MOPLock.tryLock()) {
			Thread.yield();
		}
		checkKeyValueConsistency_monitor_init_ThreadToRunnable.put(t, r);
		checkKeyValueConsistency_MOPLock.unlock();
	}

	before (Thread t): ( execution(void Thread+.run()) && target(t) ) && MOP_CommonPointCut() {
		if(Thread.currentThread() == t) {
			checkKeyValueConsistencyRuntimeMonitor.monitor_initEvent();
		}
	}

	before (Runnable r): ( execution(void Runnable+.run()) && !execution(void Thread+.run()) && target(r) ) && MOP_CommonPointCut() {
		while (!checkKeyValueConsistency_MOPLock.tryLock()) {
			Thread.yield();
		}
		if(checkKeyValueConsistency_monitor_init_ThreadToRunnable.get(Thread.currentThread()) == r) {
			checkKeyValueConsistencyRuntimeMonitor.monitor_initEvent();
		}
		checkKeyValueConsistency_MOPLock.unlock();
	}

	before (): (execution(void *.main(..)) ) && MOP_CommonPointCut() {
		if(checkKeyValueConsistency_monitor_init_MainThread == null){
			checkKeyValueConsistency_monitor_init_MainThread = Thread.currentThread();
			checkKeyValueConsistencyRuntimeMonitor.monitor_initEvent();
		}
	}

}
