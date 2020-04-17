package com.jeffdisher.laminar.disk;

import com.jeffdisher.laminar.types.EventRecord;


/**
 * Callbacks sent by the DiskManager, on its thread (implementor will need to hand these off to a different thread).
 */
public interface IDiskManagerBackgroundCallbacks {
	/**
	 * A previously requested event commit operation has completed.
	 * 
	 * @param completed The record which has now committed.
	 */
	void eventWasCommitted(EventRecord completed);

	/**
	 * A previously requested event record has been fetched.
	 * 
	 * @param record The record which was fetched from storage.
	 */
	void eventWasFetched(EventRecord record);
}
