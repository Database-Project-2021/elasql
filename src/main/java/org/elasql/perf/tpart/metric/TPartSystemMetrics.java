package org.elasql.perf.tpart.metric;

import org.elasql.perf.MetricReport;

public class TPartSystemMetrics implements MetricReport {
	
	private static final long serialVersionUID = 20210831001L;

	public static class Builder {
		private int serverId;

		private double bufferHitRate;
		private double bufferAvgPinCount;
		private int pinnedBufferCount;
		
		private double systemCpuLoad;
		private double processCpuLoad;
		private double systemLoadAverage;
		
		private int threadActiveCount;
		
		public Builder(int serverId) {
			this.serverId = serverId;
		}
		
		public void setBufferHitRate(double bufferHitRate) {
			this.bufferHitRate = bufferHitRate;
		}
		
		public void setBufferAvgPinCount(double avgPinCount) {
			this.bufferAvgPinCount = avgPinCount;
		}
		
		public void setPinnedBufferCount(int pinnedBufferCount) {
			this.pinnedBufferCount = pinnedBufferCount;
		}
		
		public void setSystemCpuLoad(double systemCpuLoad) {
			this.systemCpuLoad = systemCpuLoad;
		}
		
		public void setProcessCpuLoad(double processCpuLoad) {
			this.processCpuLoad = processCpuLoad;
		}
		
		public void setSystemLoadAverage(double systemLoadAverage) {
			this.systemLoadAverage = systemLoadAverage;
		}
		
		public void setThreadActiveCount(int threadActiveCount) {
			this.threadActiveCount = threadActiveCount;
		}
		
		public TPartSystemMetrics build() {
			TPartSystemMetrics metrics = new TPartSystemMetrics();
			metrics.serverId = serverId;
			
			metrics.bufferHitRate = bufferHitRate;
			metrics.bufferAvgPinCount = bufferAvgPinCount;
			metrics.pinnedBufferCount = pinnedBufferCount;
			
			metrics.systemCpuLoad = systemCpuLoad;
			metrics.processCpuLoad = processCpuLoad;
			metrics.systemLoadAverage = systemLoadAverage;
			
			metrics.threadActiveCount = threadActiveCount;
			return metrics;
		}
	}
	
	private int serverId;

	private double bufferHitRate;
	private double bufferAvgPinCount;
	private int pinnedBufferCount;
	
	private double systemCpuLoad;
	private double processCpuLoad;
	private double systemLoadAverage;
	
	private int threadActiveCount;
	
	private TPartSystemMetrics() {
		// do nothing
	}
	
	public int getServerId() {
		return serverId;
	}
	
	public double getBufferHitRate() {
		return bufferHitRate;
	}
	
	public double getBufferAvgPinCount() {
		return bufferAvgPinCount;
	}
	
	public int getPinnedBufferCount() {
		return pinnedBufferCount;
	}
	
	public double getSystemCpuLoad() {
		return systemCpuLoad;
	}
	
	public double getProcessCpuLoad() {
		return processCpuLoad;
	}
	
	public double getSystemLoadAverage() {
		return systemLoadAverage;
	}
	
	public int getThreadActiveCount() {
		return threadActiveCount;
	}
}
