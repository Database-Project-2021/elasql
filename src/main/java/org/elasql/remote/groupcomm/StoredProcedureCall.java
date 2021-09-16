/*******************************************************************************
 * Copyright 2016, 2018 elasql.org contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package org.elasql.remote.groupcomm;

import java.io.Serializable;
import java.util.Arrays;

/**
 * 
 * This class defines a stored procedure call.
 * 
 */
public class StoredProcedureCall implements Serializable {

	public static int PID_NO_OPERATION = Integer.MIN_VALUE;

	private static final long serialVersionUID = 8807383803517134106L;

	private Object[] objs;

	private long txNum = -1;

	private int clientId, pid = PID_NO_OPERATION, connectionId = -1;
	
	// The timestamp to indicate the time that this request arrives
	// at the database system. (-1 means 'not set')
	private long arrivedTime = -1, ou0StartTime = -1, ou0StopTime = -1;

	public static StoredProcedureCall getNoOpStoredProcCall(int clienId) {
		return new StoredProcedureCall(clienId);
	}

	StoredProcedureCall(int clienId) {
		this.clientId = clienId;
	}

	public StoredProcedureCall(int clienId, int pid, Object... objs) {
		this.clientId = clienId;
		this.pid = pid;
		this.objs = objs;
	}

	public StoredProcedureCall(int clientId, int connId, int pid, Object... objs) {
		this.clientId = clientId;
		this.connectionId = connId;
		this.pid = pid;
		this.objs = objs;
	}

	public Object[] getPars() {
		return objs;
	}

	public long getTxNum() {
		return txNum;
	}

	public void setTxNum(long txNum) {
		this.txNum = txNum;
	}
	
	public void stampArrivedTime(long timestamp) {
		if (arrivedTime == -1)
			arrivedTime = timestamp;
	}

	public void stampOu0StartTime(long ou0StartTime) {
		if (this.ou0StartTime == -1)
			this.ou0StartTime = ou0StartTime;
	}
	
	public void stampOu0StopTime(long ou0StopTime) {
		if (this.ou0StopTime == -1)
			this.ou0StopTime = ou0StopTime;
	}
	
	public long getProfiler() {
		return arrivedTime;
	}
	
	public long getArrivedTime() {
		return arrivedTime;
	}
	
	public long getOu0StartTime() {
		return ou0StartTime;
	}
	
	public long getOu0StopTime() {
		return ou0StopTime;
	}

	public int getClientId() {
		return clientId;
	}

	public int getConnectionId() {
		return connectionId;
	}

	public int getPid() {
		return pid;
	}

	public boolean isNoOpStoredProcCall() {
		return pid == PID_NO_OPERATION;
	}
	
	@Override
	public String toString() {
		return String.format("{Tx.%d, procedure id: %d, parameters: %s, from no.%d connection of client node %d}",
				txNum, pid, Arrays.toString(objs), connectionId, clientId);
	}
}
