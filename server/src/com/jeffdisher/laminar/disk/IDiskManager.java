package com.jeffdisher.laminar.disk;

import com.jeffdisher.laminar.types.EventRecord;
import com.jeffdisher.laminar.types.MutationRecord;


/**
 * Interface of DiskManager to make unit testing NodeState easier.
 */
public interface IDiskManager {
	/**
	 * Requests that the event with the associated localOffset offset be asynchronously fetched.
	 * NOTE:  This will be changed to per-topic fetch, in the future.
	 * 
	 * @param localOffset The offset of the event to load.
	 */
	void fetchEvent(long localOffset);

	/**
	 * Request that the given event be asynchronously committed.
	 * NOTE:  This will be changed to per-topic commit, in the future.
	 * 
	 * @param event The event to commit.
	 */
	void commitEvent(EventRecord event);

	/**
	 * Request that the given mutation be asynchronously committed.
	 * 
	 * @param mutation The mutation to commit.
	 */
	void commitMutation(MutationRecord mutation);

	/**
	 * Requests that the mutation with the associated global offset be asynchronously fetched.
	 * 
	 * @param globalOffset The offset of the mutation to load.
	 */
	void fetchMutation(long mutationOffset);
}
