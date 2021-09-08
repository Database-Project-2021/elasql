package org.elasql.perf.tpart.workload;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasql.storage.metadata.PartitionMetaMgr;

/**
 * An object to store the features for a transaction request.
 * 
 * @author Yu-Xuan Lin, Yu-Shan Lin
 */
public class TransactionFeatures {
	
	// Defines a read-only list for feature keys
	public static final List<String> FEATURE_KEYS;
	public static final int SERVER_COUNT = PartitionMetaMgr.NUM_PARTITIONS;
	
	static {
		List<String> featureKeys = new ArrayList<String>();

		// Transaction Features:
		// (Modify this part to add/remove features)
		// - Transaction start time (the time entering the system)
		featureKeys.add("Start Time");
		// - Number of read records
		featureKeys.add("Number of Read Records");
		// - Number of written records
		featureKeys.add("Number of Write Records");

		// - Number of arithmetic operations of a transaction
		featureKeys.add("Number of Arithmetic Operations");
		// - Number of read adn written records
		featureKeys.add("Number of Read Write Records");
		// - Number of cached writes
		featureKeys.add("Number of Cache Write");
		// - Number of cached inserts
		featureKeys.add("Number of Cache Insert");
		
		addKeysWithServerCount(featureKeys, "System CPU Load");
		addKeysWithServerCount(featureKeys, "Process CPU Load");
		addKeysWithServerCount(featureKeys, "System Load Average");
		addKeysWithServerCount(featureKeys, "Thread Active Count");
		
		// Convert the list to a read-only list
		FEATURE_KEYS = Collections.unmodifiableList(featureKeys);
	}
	
	public static void addKeysWithServerCount(List<String> list, String key) {
		for (int serverId = 0; serverId < SERVER_COUNT; serverId++) {
			String keyWithServerId = getKeyWithServerId(key, serverId);
			list.add(keyWithServerId);
		}
	}
	
	public static String getKeyWithServerId(String key, int serverId) {
		// %-3d means the field width is 3 and it is left justification
		return String.format("%s - Server %d", key, serverId);
	}
	
	// Builder Pattern
	// - avoids passing Map and List from outside
	// - creates immutable TransactionFeatures objects
	// - checks the correctness before building an object
	public static class Builder {
		private long txNum;
		private Map<String, Object> features;
		private List<Long> dependentTxns;
		
		public Builder(long txNum) {
			this.txNum = txNum;
			this.features = new HashMap<String, Object>();
			this.dependentTxns = new ArrayList<Long>();
		}
		
		public void addFeature(String key, Object value) {
			if (!FEATURE_KEYS.contains(key))
				throw new RuntimeException("Unexpected feature: " + key);
			
			features.put(key, value);
		}
		
		public void addFeatureWithServerId(String key, Object value, int serverId) {
			String keyWithServerId = getKeyWithServerId(key, serverId);
			if (!FEATURE_KEYS.contains(keyWithServerId))
				throw new RuntimeException("Unexpected feature: " + keyWithServerId);
			features.put(keyWithServerId, value);
		}
		
		public void addDependency(Long dependentTxNum) {
			if (dependentTxNum >= txNum)
				throw new RuntimeException(
						String.format("Tx.%d should not depend to tx.%d", txNum, dependentTxNum));
			
			dependentTxns.add(dependentTxNum);
		}
		
		public TransactionFeatures build() {
			// Check the integrity of the features
			for (String key : FEATURE_KEYS)
				if (!features.containsKey(key))
					throw new RuntimeException(
							String.format("Feature '%s' is missing for tx.%d", key, txNum));
			
			// Sort the dependencies
			Collections.sort(dependentTxns);
			
			return new TransactionFeatures(txNum, features, dependentTxns);
		}
	}
	
	private long txNum;
	private Map<String, Object> features;
	// Transaction dependencies are handled separately
	private List<Long> dependentTxns;
	
	// Builder Pattern: set the constructor to private to avoid creating an object from outside
	private TransactionFeatures(long txNum, Map<String, Object> features, List<Long> dependentTxns) {
		this.txNum = txNum;
		this.features = features;
		this.dependentTxns = dependentTxns;
	}
	
	public long getTxNum() {
		return txNum;
	}
	
	public Object getFeature(String key) {
		return features.get(key);
	}
	
	public List<Long> getDependencies() {
		// Use 'unmodifiableList' to avoid the list is modified outside
		return Collections.unmodifiableList(dependentTxns);
	}
}
