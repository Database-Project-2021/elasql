/*******************************************************************************
 * Copyright 2016 vanilladb.org
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package org.elasql.storage.tx.concurrency;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.vanilladb.core.storage.tx.concurrency.LockAbortException;
import org.vanilladb.core.util.TransactionProfiler;

public class ConservativeOrderedLockTable {

	private static final int NUM_ANCHOR = 1009;
	private static Logger logger = Logger.getLogger(ConservativeOrderedLockTable.class.getName());
	
	enum LockType {
		IS_LOCK, IX_LOCK, S_LOCK, SIX_LOCK, X_LOCK
	}

	private class Lockers {
		static final long NONE = -1; // for sixLocker, xLocker and wbLocker
		
		List<Long> sLockers, ixLockers, isLockers;
		// only one tx can hold xLock(sixLock) on single item
		long sixLocker, xLocker;
		LinkedList<Long> requestQueue;

		Lockers() {
			sLockers = new LinkedList<Long>();
			ixLockers = new LinkedList<Long>();
			isLockers = new LinkedList<Long>();
			sixLocker = NONE;
			xLocker = NONE;
			requestQueue = new LinkedList<Long>();
		}
		
		@Override
		public String toString() {
			return "{S: " + sLockers +
					", IX: " + ixLockers +
					", IS: " + isLockers +
					", SIX: " + sixLocker +
					", X: " + xLocker +
					", requests: " + requestQueue +
					"}";
		}
	}

	private Map<Object, Lockers> lockerMap = new ConcurrentHashMap<Object, Lockers>();

	// Lock-stripping
	private final Object anchors[] = new Object[NUM_ANCHOR];
	private final List<Queue<Object>> anchorQueues = new ArrayList<Queue<Object>>();

	/**
	 * Create and initialize a conservative ordered lock table.
	 */
	public ConservativeOrderedLockTable() {
		// Initialize anchors
		for (int i = 0; i < anchors.length; ++i) {
			anchors[i] = new Object();
		}

		for (int i = 0; i < anchors.length; ++i) {
			anchorQueues.add(new LinkedList<Object>());
		}
	}

	private void logInfo(String msg){
		if (logger.isLoggable(Level.INFO)) 
			logger.info(msg);
	}

	/**
	 * Request lock for an object. This method will put the requested
	 * transaction into a waiting queue of requested object.
	 * 
	 * @param obj
	 *            the object which transaction request lock for
	 * @param txNum
	 *            the transaction that requests the lock
	 */
	void requestLock(Object obj, long txNum) {
		synchronized (getAnchor(obj)) {
			Lockers lockers = prepareLockers(obj);
			lockers.requestQueue.add(txNum);
		}
	}

	/**
	 * Grants an slock on the specified item. If any conflict lock exists when
	 * the method is called, then the calling thread will be placed on a wait
	 * list until the lock is released. If the thread remains on the wait list
	 * for a certain amount of time, then an exception is thrown.
	 * 
	 * @param obj
	 *            an object to be locked
	 * @param txNum
	 *            a transaction number
	 * 
	 */
	void sLock(Object obj, long txNum) {
		System.out.println("Txn " + txNum + " S - Acquiring Lock...");
		// logInfo("S - Acquiring Lock...");
		TransactionProfiler profiler = TransactionProfiler.getLocalProfiler();
		int indicator = 3;
		boolean is_wait_anchor = false;
		profiler.startComponentProfilerIndic("OU3 - sLock Overhead", indicator);
		Object anchor = getAnchor(obj);

		profiler.startComponentProfilerIndic("OU3 - sAnchor Waiting", indicator);
		synchronized (anchor) {
			System.out.println("Txn " + txNum + " S - Acquired anchor");
			// logInfo("S - Acquiring anchor...");
			profiler.stopComponentProfilerIndic("OU3 - sAnchor Waiting", indicator);
			Lockers lockers = prepareLockers(obj);

			// synchronized(lockers.requestQueue){
				// check if it have already held the lock
				if (hasSLock(lockers, txNum)) {
					lockers.requestQueue.remove(txNum);
					return;
				}
			// try {
				Long head = lockers.requestQueue.peek();
				while (!sLockable(lockers, txNum) || (head != null && head.longValue() != txNum)) {
					profiler.stopComponentProfilerIndic("OU3 - sLock Overhead", indicator);
					profiler.startComponentProfilerIndic("OU3 - sLock Waiting", indicator);
					// anchor.wait();
					profiler.stopComponentProfilerIndic("OU3 - sLock Waiting", indicator);
					
					// logInfo("S - Waiting Obj...");
					System.out.println("Txn " + txNum + " S - Waiting Obj...");
					// waitObj(txNum, obj);
					waitRequestQueue(txNum, lockers.requestQueue);
					// logInfo("S - Finished waiting Obj");
					System.out.println("Txn " + txNum + " S - Finished waiting Obj");

					lockers = prepareLockers(obj);
					head = lockers.requestQueue.peek();
					is_wait_anchor = true;
				}
			
				if(!is_wait_anchor){
					profiler.stopComponentProfilerIndic("OU3 - sLock Overhead", indicator);
					profiler.startComponentProfilerIndic("OU3 - sLock Waiting", indicator);
					profiler.stopComponentProfilerIndic("OU3 - sLock Waiting", indicator);
				}
				
				if (!sLockable(lockers, txNum))
					throw new LockAbortException();
				// get the s lock
				lockers.requestQueue.poll();
				lockers.sLockers.add(txNum);
				// Wake up other waiting transactions (on this object) to let
				anchor.notifyAll();
				
				// logInfo("S - Release Obj...");
				System.out.println("Txn " + txNum + " S - Release Obj...");
				// releaseObj(txNum, obj);
				releaseRequestQueue(txNum, lockers.requestQueue);
				// logInfo("S - Released Obj...");
				System.out.println("Txn " + txNum + " S - Released Obj...");
			// } catch (InterruptedException e) {
			// 	e.printStackTrace();
			// 	throw new LockAbortException("Interrupted when waitting for lock");
			// }
			// }
		}
		System.out.println("Txn " + txNum + " S - Acquired Lock");
	}

	/**
	 * Grants an xlock on the specified item. If any conflict lock exists when
	 * the method is called, then the calling thread will be placed on a wait
	 * list until the lock is released. If the thread remains on the wait list
	 * for a certain amount of time, then an exception is thrown.
	 * 
	 * @param obj
	 *            an object to be locked
	 * @param txNum
	 *            a transaction number
	 * 
	 */
	void xLock(Object obj, long txNum) {
		System.out.println("Txn " + txNum + " X - Acquiring Lock...");
		// logInfo("X - Acquiring Lock...");
		// See the comments in sLock(..) for the explanation of the algorithm
		TransactionProfiler profiler = TransactionProfiler.getLocalProfiler();
		int indicator = 3;
		boolean is_wait_anchor = false;
		profiler.startComponentProfilerIndic("OU3 - xLock Overhead", indicator);
		Object anchor = getAnchor(obj);

		profiler.startComponentProfilerIndic("OU3 - xAnchor Waiting", indicator);
		synchronized (anchor) {
			System.out.println("Txn " + txNum + " X - Acquired anchor");
			// logInfo("X - Acquiring anchor...");
			profiler.stopComponentProfilerIndic("OU3 - xAnchor Waiting", indicator);
			Lockers lockers = prepareLockers(obj);
			
			// synchronized(lockers.requestQueue){
				if (hasXLock(lockers, txNum)) {
					lockers.requestQueue.remove(txNum);
					return;
				}
			// try {
				Long head = lockers.requestQueue.peek();
				while ((!xLockable(lockers, txNum) || (head != null && head.longValue() != txNum))) {
					profiler.stopComponentProfilerIndic("OU3 - xLock Overhead", indicator);
					profiler.startComponentProfilerIndic("OU3 - xLock Waiting", indicator);
					// anchor.wait();
					// synchronized(head){
					// 	try{
					// 		head.wait();
					// 	} catch (InterruptedException e) {
					// 		e.printStackTrace();
					// 		throw new LockAbortException("Interrupted when waiting for lock of head");
					// 	}
					// }
					profiler.stopComponentProfilerIndic("OU3 - xLock Waiting", indicator);
					
					// logInfo("X - Waiting Obj...");
					System.out.println("Txn " + txNum + " X - Waiting Obj...");
					// waitObj(txNum, obj);
					waitRequestQueue(txNum, lockers.requestQueue);
					// logInfo("X - Finished waiting Obj");
					System.out.println("Txn " + txNum + " X - Finished waiting Obj");

					lockers = prepareLockers(obj);
					head = lockers.requestQueue.peek();
					is_wait_anchor = true;
				}

				if(!is_wait_anchor){
					profiler.stopComponentProfilerIndic("OU3 - xLock Overhead", indicator);
					profiler.startComponentProfilerIndic("OU3 - xLock Waiting", indicator);
					profiler.stopComponentProfilerIndic("OU3 - xLock Waiting", indicator);
				}
				lockers.requestQueue.poll();
				lockers.xLocker = txNum;
			// } catch (InterruptedException e) {
			// 	throw new LockAbortException("Interrupted when waitting for lock");
			// }
			// }
		}
		System.out.println("Txn " + txNum + " X - Acquired Lock");
	}

	/**
	 * Grants an sixlock on the specified item. If any conflict lock exists when
	 * the method is called, then the calling thread will be placed on a wait
	 * list until the lock is released. If the thread remains on the wait list
	 * for a certain amount of time, then an exception is thrown.
	 * 
	 * @param obj
	 *            an object to be locked
	 * @param txNum
	 *            a transaction number
	 * 
	 */
	// void sixLock(Object obj, long txNum) {
	// 	// See the comments in sLock(..) for the explanation of the algorithm 
	// 	Object anchor = getAnchor(obj);
		
	// 	synchronized (anchor) {
	// 		Lockers lockers = prepareLockers(obj);

	// 		if (hasSixLock(lockers, txNum)) {
	// 			lockers.requestQueue.remove(txNum);
	// 			return;
	// 		}

	// 		try {
	// 			Long head = lockers.requestQueue.peek();
	// 			while (!sixLockable(lockers, txNum)
	// 					|| (head != null && head.longValue() != txNum)) {
	// 				anchor.wait();
	// 				lockers = prepareLockers(obj);
	// 				head = lockers.requestQueue.peek();
	// 			}

	// 			// get the six lock
	// 			lockers.requestQueue.poll();
	// 			lockers.sixLocker = txNum;
				
	// 			anchor.notifyAll();
	// 		} catch (InterruptedException e) {
	// 			throw new LockAbortException(
	// 					"Interrupted when waitting for lock");
	// 		}
	// 	}
	// }

	/**
	 * Grants an islock on the specified item. If any conflict lock exists when
	 * the method is called, then the calling thread will be placed on a wait
	 * list until the lock is released. If the thread remains on the wait list
	 * for a certain amount of time, then an exception is thrown.
	 * 
	 * @param obj
	 *            an object to be locked
	 * @param txNum
	 *            a transaction number
	 */
	// void isLock(Object obj, long txNum) {
	// 	// See the comments in sLock(..) for the explanation of the algorithm 
	// 	Object anchor = getAnchor(obj);
		
	// 	synchronized (anchor) {
	// 		Lockers lockers = prepareLockers(obj);

	// 		if (hasIsLock(lockers, txNum)) {
	// 			lockers.requestQueue.remove(txNum);
	// 			return;
	// 		}

	// 		try {
	// 			Long head = lockers.requestQueue.peek();
	// 			while (!isLockable(lockers, txNum)
	// 					|| (head != null && head.longValue() != txNum)) {
	// 				anchor.wait();
	// 				lockers = prepareLockers(obj);
	// 				head = lockers.requestQueue.peek();
	// 			}

	// 			// get the is lock
	// 			lockers.requestQueue.poll();
	// 			lockers.isLockers.add(txNum);
				
	// 			anchor.notifyAll();
	// 		} catch (InterruptedException e) {
	// 			throw new LockAbortException(
	// 					"Interrupted when waitting for lock");
	// 		}
	// 	}
	// }

	/**
	 * Grants an ixlock on the specified item. If any conflict lock exists when
	 * the method is called, then the calling thread will be placed on a wait
	 * list until the lock is released. If the thread remains on the wait list
	 * for a certain amount of time, then an exception is thrown.
	 * 
	 * @param obj
	 *            an object to be locked
	 * @param txNum
	 *            a transaction number
	 */
	// void ixLock(Object obj, long txNum) {
	// 	// See the comments in sLock(..) for the explanation of the algorithm 
	// 	Object anchor = getAnchor(obj);
		
	// 	synchronized (anchor) {
	// 		Lockers lockers = prepareLockers(obj);

	// 		if (hasIxLock(lockers, txNum)) {
	// 			lockers.requestQueue.remove(txNum);
	// 			return;
	// 		}

	// 		try {
	// 			Long head = lockers.requestQueue.peek();
	// 			while (!ixLockable(lockers, txNum)
	// 					|| (head != null && head.longValue() != txNum)) {
	// 				anchor.wait();
	// 				lockers = prepareLockers(obj);
	// 				head = lockers.requestQueue.peek();
	// 			}

	// 			// get the ix lock
	// 			lockers.requestQueue.poll();
	// 			lockers.ixLockers.add(txNum);
				
	// 			anchor.notifyAll();
	// 		} catch (InterruptedException e) {
	// 			throw new LockAbortException(
	// 					"Interrupted when waitting for lock");
	// 		}
	// 	}
	// }

	/**
	 * Releases the specified type of lock on an item holding by a transaction.
	 * If a lock is the last lock on that block, then the waiting transactions
	 * are notified.
	 * 
	 * @param obj
	 *            a locked object
	 * @param txNum
	 *            a transaction number
	 * @param lockType
	 *            the type of lock
	 */
	void release(Object obj, long txNum, LockType lockType) {
		Object anchor = getAnchor(obj);
		Queue<Object> anchorQueue = getQueue(obj);
		synchronized (anchor) {
			Lockers lks = lockerMap.get(obj);
			
			if (lks == null)
				return;
			
			System.out.println("Releasing lock...");
			releaseLock(lks, txNum, lockType, anchor, anchorQueue, obj);
			System.out.println("Released lock");

			// Remove the locker, if there is no other transaction
			// holding it
			if (!sLocked(lks) && !xLocked(lks) && !sixLocked(lks)
					&& !isLocked(lks) && !ixLocked(lks)
					&& lks.requestQueue.isEmpty())
				lockerMap.remove(obj);
			
			// There might be someone waiting for the lock
			anchor.notifyAll();
			System.out.println("Releasing obj...");
			// releaseObj(txNum, obj);
			// releaseRequestQueue(txNum, lks.requestQueue);
			System.out.println("Released obj...");
		}
	}

	/**
	 * Gets the anchor for the specified object.
	 * 
	 * @param obj
	 *            the target object
	 * @return the anchor for obj
	 */
	private Object getAnchor(Object obj) {
		int code = obj.hashCode();
		code = Math.abs(code); // avoid negative value
		return anchors[code % anchors.length];
	}

	private void waitRequestQueue(long txNum, LinkedList<Long> requestQueue){
		if(requestQueue.contains(txNum)){
			int idx = requestQueue.indexOf(txNum);
			Long obj = requestQueue.get(idx);
			synchronized(obj){
				try{
					obj.wait();
				} catch (InterruptedException e) {
					e.printStackTrace();
					throw new LockAbortException("Interrupted when locking requestQueue");
				}
			}
		}else{
			System.out.println("requestQueue is empty, Txn " + txNum + " isn't inside the queue");
		}
	}

	private void releaseRequestQueue(long txNum, LinkedList<Long> requestQueue){
		// synchronized(requestQueue){
			// if(requestQueue.contains(txNum)){
				// int idx = requestQueue.indexOf(txNum);
				// Long obj = requestQueue.get(idx);
				Long obj = requestQueue.peek();
				if(obj != null){
					obj.notifyAll();
				}else{
					System.out.println("requestQueue is empty, cannot notifyAll()");
				}
			// }
		// }
	}

	private Queue<Object> getQueue(Object obj) {
		int code = obj.hashCode();
		code = Math.abs(code); // avoid negative value
		return anchorQueues.get(code % anchors.length);
	}

	private void lockObj(Object obj, Queue<Object> anchorQueue){
		System.out.println(obj.toString() + " Locking obj...");
		synchronized(obj){
			synchronized(anchorQueue){
				for(Object item: anchorQueue){
					if(item.equals(obj)){
						return;
					}
				}

				System.out.println(obj.toString() + " - " + obj.toString() + " - " + " Entering queue...");
				anchorQueue.add(obj);
				System.out.println(obj.toString() + " - " + obj.toString() + " - " + " Entered queue | size: " + anchorQueue.size());
			}

			try{
				System.out.println(obj.toString() + " Waiting...");
				obj.wait();
				System.out.println(obj.toString() + " Exited wait");
			} catch (InterruptedException e) {
				e.printStackTrace();
				throw new LockAbortException("Interrupted when locking object");
			}
		}
	}

	private void waitObj(Long txNum, Object obj){
		Queue<Object> anchorQueue = getQueue(obj);
		lockObj(txNum, anchorQueue);
	}

	// private void releaseQueue(Queue<Object> anchorQueue){
	// 	Object wakeupObj = anchorQueue.poll();
	// 	if(wakeupObj != null){
	// 		wakeupObj.notifyAll();
	// 	}
	// }

	private void releaseObj(Long txNum, Object obj){
		Queue<Object> anchorQueue = getQueue(obj);
		synchronized(anchorQueue){
			Object wakeupObj = anchorQueue.poll();
			System.out.println("Txn " + txNum + " - " + obj.toString() + " - " + " Queue size: " + anchorQueue.size());

			if(wakeupObj != null){
				System.out.println(wakeupObj.toString() + " Waking up");
				synchronized(wakeupObj){
					// try{
					wakeupObj.notifyAll();
					System.out.println(wakeupObj.toString() + " Notified");
					// } catch (InterruptedException e) {
					// 	e.printStackTrace();
					// 	throw new LockAbortException("Interrupted when locking object");
					// }
				}
			}else{
				System.out.println("wakeupObj is null");
			}
		}
	}

	private Lockers prepareLockers(Object obj) {
		Lockers lockers = lockerMap.get(obj);
		if (lockers == null) {
			lockers = new Lockers();
			lockerMap.put(obj, lockers);
		}
		return lockers;
	}

	private void releaseLock(Lockers lks, long txNum, LockType lockType,
			Object anchor, Queue<Object> anchorQueue, Object obj) {
		switch (lockType) {
		case X_LOCK:
			if (lks.xLocker == txNum) {
				lks.xLocker = -1;
				anchor.notifyAll();
				// releaseObj(txNum, obj);
				releaseRequestQueue(txNum, lks.requestQueue);
			}
			return;
		case SIX_LOCK:
			if (lks.sixLocker == txNum) {
				lks.sixLocker = -1;
				anchor.notifyAll();
				// releaseObj(txNum, obj);
				releaseRequestQueue(txNum, lks.requestQueue);
			}
			return;
		case S_LOCK:
			List<Long> sl = lks.sLockers;
			if (sl != null && sl.contains(txNum)) {
				sl.remove((Long) txNum);
				if (sl.isEmpty())
					anchor.notifyAll();
					// releaseObj(txNum, obj);
					releaseRequestQueue(txNum, lks.requestQueue);
			}
			return;
		case IS_LOCK:
			List<Long> isl = lks.isLockers;
			if (isl != null && isl.contains(txNum)) {
				isl.remove((Long) txNum);
				if (isl.isEmpty())
					anchor.notifyAll();
					// releaseObj(txNum, obj);
					releaseRequestQueue(txNum, lks.requestQueue);
			}
			return;
		case IX_LOCK:
			List<Long> ixl = lks.ixLockers;
			if (ixl != null && ixl.contains(txNum)) {
				ixl.remove((Long) txNum);
				if (ixl.isEmpty())
					anchor.notifyAll();
					// releaseObj(txNum, obj);
					releaseRequestQueue(txNum, lks.requestQueue);
			}
			return;
		default:
			throw new IllegalArgumentException();
		}
	}

	/*
	 * Verify if an item is locked.
	 */

	private boolean sLocked(Lockers lks) {
		return lks != null && lks.sLockers.size() > 0;
	}

	private boolean xLocked(Lockers lks) {
		return lks != null && lks.xLocker != -1;
	}

	private boolean sixLocked(Lockers lks) {
		return lks != null && lks.sixLocker != -1;
	}

	private boolean isLocked(Lockers lks) {
		return lks != null && lks.isLockers.size() > 0;
	}

	private boolean ixLocked(Lockers lks) {
		return lks != null && lks.ixLockers.size() > 0;
	}

	/*
	 * Verify if an item is held by a tx.
	 */

	private boolean hasSLock(Lockers lks, long txNum) {
		return lks != null && lks.sLockers.contains(txNum);
	}

	private boolean hasXLock(Lockers lks, long txNUm) {
		return lks != null && lks.xLocker == txNUm;
	}

	private boolean hasSixLock(Lockers lks, long txNum) {
		return lks != null && lks.sixLocker == txNum;
	}

	private boolean hasIsLock(Lockers lks, long txNum) {
		return lks != null && lks.isLockers.contains(txNum);
	}

	private boolean hasIxLock(Lockers lks, long txNum) {
		return lks != null && lks.ixLockers.contains(txNum);
	}

	private boolean isTheOnlySLocker(Lockers lks, long txNum) {
		return lks != null && lks.sLockers.size() == 1
				&& lks.sLockers.contains(txNum);
	}

	private boolean isTheOnlyIsLocker(Lockers lks, long txNum) {
		if (lks != null) {
			for (Object o : lks.isLockers)
				if (!o.equals(txNum))
					return false;
			return true;
		}
		return false;
	}

	private boolean isTheOnlyIxLocker(Lockers lks, long txNum) {
		if (lks != null) {
			for (Object o : lks.ixLockers)
				if (!o.equals(txNum))
					return false;
			return true;
		}
		return false;
	}

	/*
	 * Verify if an item is lockable to a tx.
	 */

	private boolean sLockable(Lockers lks, long txNum) {
		return (!xLocked(lks) || hasXLock(lks, txNum))
				&& (!sixLocked(lks) || hasSixLock(lks, txNum))
				&& (!ixLocked(lks) || isTheOnlyIxLocker(lks, txNum));
	}

	private boolean xLockable(Lockers lks, long txNum) {
		return (!sLocked(lks) || isTheOnlySLocker(lks, txNum))
				&& (!sixLocked(lks) || hasSixLock(lks, txNum))
				&& (!ixLocked(lks) || isTheOnlyIxLocker(lks, txNum))
				&& (!isLocked(lks) || isTheOnlyIsLocker(lks, txNum))
				&& (!xLocked(lks) || hasXLock(lks, txNum));
	}

	private boolean sixLockable(Lockers lks, long txNum) {
		return (!sixLocked(lks) || hasSixLock(lks, txNum))
				&& (!ixLocked(lks) || isTheOnlyIxLocker(lks, txNum))
				&& (!sLocked(lks) || isTheOnlySLocker(lks, txNum))
				&& (!xLocked(lks) || hasXLock(lks, txNum));
	}

	private boolean ixLockable(Lockers lks, long txNum) {
		return (!sLocked(lks) || isTheOnlySLocker(lks, txNum))
				&& (!sixLocked(lks) || hasSixLock(lks, txNum))
				&& (!xLocked(lks) || hasXLock(lks, txNum));
	}

	private boolean isLockable(Lockers lks, long txNum) {
		return (!xLocked(lks) || hasXLock(lks, txNum));
	}
}