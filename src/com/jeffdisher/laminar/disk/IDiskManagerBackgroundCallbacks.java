package com.jeffdisher.laminar.disk;

import com.jeffdisher.laminar.types.EventRecord;
import com.jeffdisher.laminar.types.MutationRecord;


/**
 * Callbacks sent by the DiskManager, on its thread (implementor will need to hand these off to a different thread).
 */
public interface IDiskManagerBackgroundCallbacks {
	/**
	 * A previously requested mutation commit operation has completed.
	 * 
	 * @param completed The record which has now committed.
	 */
	void mutationWasCommitted(MutationRecord completed);

	/**
	 * A previously requested event commit operation has completed.
	 * 
	 * @param completed The record which has now committed.
	 */
	void eventWasCommitted(EventRecord completed);

	/**
	 * A previously requested mutation record has been fetched.
	 * 
	 * @param record The record which was fetched from storage.
	 */
	void mutationWasFetched(MutationRecord record);

	/**
	 * A previously requested event record has been fetched.
	 * 
	 * @param record The record which was fetched from storage.
	 */
	void eventWasFetched(EventRecord record);
}
