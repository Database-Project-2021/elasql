package org.elasql.procedure.tpart;

import java.util.Set;

import org.elasql.procedure.StoredProcedureTask;
import org.elasql.procedure.tpart.TPartStoredProcedure.ProcedureType;
import org.elasql.schedule.tpart.sink.SunkPlan;
import org.elasql.server.Elasql;
import org.elasql.sql.PrimaryKey;
import org.elasql.util.FeatureCollector;
import org.elasql.util.SystemInfo;
import org.elasql.util.TransactionFeaturesRecorder;
import org.elasql.util.TransactionStatisticsRecorder;
import org.vanilladb.core.remote.storedprocedure.SpResultSet;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.server.task.Task;
import org.vanilladb.core.server.task.TaskMgr;
import org.vanilladb.core.util.Timer;

public class TPartStoredProcedureTask
		extends StoredProcedureTask<TPartStoredProcedure<?>> {
	
	private static class WaitingForStartingRecordTask extends Task {
//		private static final int WARM_UP_TIME = 100_000;
		private static final int WARM_UP_TIME = 0; // in milliseconds
		@Override
		public void run() {
			try {
				Thread.sleep(WARM_UP_TIME);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			TransactionStatisticsRecorder.startRecording();
			TransactionFeaturesRecorder.startRecording();
		}
	}
	
	static {
		// For Debugging
//		TimerStatistics.startReporting();
//		TransactionStatisticsRecorder.startRecording();
		VanillaDb.taskMgr().runTask(new WaitingForStartingRecordTask());
	}
	
	private static long firstTxStartTime;
	
	public static void setFirstTxStartTime(long firstTxStartTime) {
		TPartStoredProcedureTask.firstTxStartTime = firstTxStartTime;
	}

	private TPartStoredProcedure<?> tsp;
	private int clientId, connectionId, parId;
	private long txNum;
	private long txStartTime, sinkStartTime, sinkStopTime, threadInitStartTime;

	public TPartStoredProcedureTask(int cid, int connId, long txNum, TPartStoredProcedure<?> sp) {
		super(cid, connId, txNum, sp);
		this.clientId = cid;
		this.connectionId = connId;
		this.txNum = txNum;
		this.tsp = sp;		
	}

	@Override
	public void run() {
		SpResultSet rs = null;

		Thread.currentThread().setName("Tx." + txNum);

		// Initialize a thread-local timer
		Timer timer = Timer.getLocalTimer();
		timer.reset();
		// MODIFIED:
		timer.recordCurrentTime("Txn Start TimeStamp");

		// MODIFIED: Cannot be fixed by merge conflict
		timer.setStartExecutionTime(txStartTime);
		timer.startComponentTimer("Generate plan", sinkStartTime);
		timer.stopComponentTimer("Generate plan", sinkStopTime);
		timer.startComponentTimer("Init thread", threadInitStartTime);
		timer.stopComponentTimer("Init thread");
		// timer.startExecution();

		// Initialize a thread-local feature collector
		FeatureCollector collector = FeatureCollector.getLocalFeatureCollector();
		collector.setFeatureValue(FeatureCollector.keys[0], (txStartTime - firstTxStartTime));
		
		// OU1: Generating Execution Plan
		collector.setFeatureValue(FeatureCollector.keys[1], tsp.getReadSet().size());
		collector.setFeatureValue(FeatureCollector.keys[2], tsp.getWriteSet().size());

		// OU2: Initialize A Thread
		collector.setFeatureValue(FeatureCollector.keys[3], Elasql.txMgr().getActiveTxCount());
		collector.setFeatureValue(FeatureCollector.keys[4], TaskMgr.THREAD_POOL_SIZE);
		collector.setFeatureValue(FeatureCollector.keys[5], SystemInfo.getCpuUsage());

		// OU8 - Transaction Commit
		collector.setFeatureValue(FeatureCollector.keys[12], tsp.getReadWriteSetSize());
		collector.setFeatureValue(FeatureCollector.keys[13], tsp.getReadWriteSetByte());

		// collector.setFeatureValue(FeatureCollector.keys[1], tsp.getReadSet());
		// collector.setFeatureValue(FeatureCollector.keys[2], tsp.getWriteSet());
		
		rs = tsp.execute();
		
		// OU6: Execute Procedure Logic
		collector.setFeatureValue(FeatureCollector.keys[6], tsp.getReadKeyNum());
		collector.setFeatureValue(FeatureCollector.keys[7], tsp.getInsertKeyNum());
		collector.setFeatureValue(FeatureCollector.keys[8], tsp.getUpdateKeyNum());
		collector.setFeatureValue(FeatureCollector.keys[9], tsp.getArithNum());

		// OU7: Write To Storage
		collector.setFeatureValue(FeatureCollector.keys[10], tsp.getWriteSetSize());
		collector.setFeatureValue(FeatureCollector.keys[11], tsp.getWriteSetByte());
		
		// Record the feature result
		TransactionFeaturesRecorder.recordResult(txNum, collector);
			
		if (tsp.isMaster()) {
			if (clientId != -1)
				Elasql.connectionMgr().sendClientResponse(clientId, connectionId, txNum, rs);

			// TODO: Uncomment this when the migration module is migrated
			// if (tsp.getProcedureType() == ProcedureType.MIGRATION) {
			// // Send a notification to the sequencer
			// TupleSet ts = new TupleSet(MigrationMgr.MSG_COLD_FINISH);
			// Elasql.connectionMgr().pushTupleSet(PartitionMetaMgr.NUM_PARTITIONS, ts);
			// }

			// For Debugging
			// timer.addToGlobalStatistics();
		}

		// Stop the timer for the whole execution
		timer.stopExecution();
		// MODIFIED:
		timer.recordCurrentTime("Txn End TimeStamp");
		// MODIFIED:
		Elasql.getTransactionGraph().addNode(txNum, timer.getExecutionTime(), System.nanoTime(), tsp.getDependenTxns());
		// Record the timer result
		TransactionStatisticsRecorder.recordResult(txNum, timer);
	}

	public long getTxNum() {
		return txNum;
	}

	public Set<PrimaryKey> getReadSet() {
		return tsp.getReadSet();
	}

	public Set<PrimaryKey> getWriteSet() {
		return tsp.getWriteSet();
	}

	public double getWeight() {
		return tsp.getWeight();
	}

	public int getPartitionId() {
		return parId;
	}

	public void setPartitionId(int parId) {
		this.parId = parId;
	}

	public void decideExceutionPlan(SunkPlan plan) {
		tsp.decideExceutionPlan(plan);
	}

	public TPartStoredProcedure<?> getProcedure() {
		return tsp;
	}

	public ProcedureType getProcedureType() {
		if (tsp == null)
			return ProcedureType.NOP;
		return tsp.getProcedureType();
	}

	public boolean isReadOnly() {
		return tsp.isReadOnly();
	}

	public void setStartTime(long txStartTime, long sinkStartTime, long sinkStopTime, long threadInitStartTime) {
		this.txStartTime = txStartTime;
		this.sinkStartTime = sinkStartTime;
		this.sinkStopTime = sinkStopTime;
		this.threadInitStartTime = threadInitStartTime;
	}
}
