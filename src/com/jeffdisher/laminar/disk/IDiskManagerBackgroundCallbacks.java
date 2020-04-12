package com.jeffdisher.laminar.disk;

import com.jeffdisher.laminar.types.EventRecord;


/**
 * Callbacks sent by the DiskManager, on its thread (implementor will need to hand these off to a different thread).
 */
public interface IDiskManagerBackgroundCallbacks {
	/**
	 * A previously requested commit operation has completed.
	 * 
	 * @param completed The record which has now committed.
	 */
	void recordWasCommitted(EventRecord completed);

	/**
	 * A previously requested record has been fetched.
	 * 
	 * @param record The record which was fetched from storage.
	 */
	void recordWasFetched(EventRecord record);
}
