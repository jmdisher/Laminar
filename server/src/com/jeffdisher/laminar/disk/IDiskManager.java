package com.jeffdisher.laminar.disk;

import com.jeffdisher.laminar.types.Consequence;
import com.jeffdisher.laminar.types.TopicName;


/**
 * Interface of DiskManager to make unit testing NodeState easier.
 */
public interface IDiskManager {
	/**
	 * Requests that the consequence with the associated localOffset offset be asynchronously fetched.
	 * 
	 * @param topic The topic where to search.
	 * @param localOffset The offset of the consequence to load.
	 */
	void fetchConsequence(TopicName topic, long localOffset);

	/**
	 * Request that the given consequence be asynchronously committed.
	 * NOTE:  This will be changed to per-topic commit, in the future.
	 * 
	 * @param topic The topic where the consequence occurred.
	 * @param consequence The consequence to commit.
	 */
	void commitConsequence(TopicName topic, Consequence consequence);

	/**
	 * Request that the given mutation be asynchronously committed.
	 * 
	 * @param mutation The mutation to commit.
	 */
	void commitMutation(CommittedMutationRecord mutation);

	/**
	 * Requests that the mutation with the associated global offset be asynchronously fetched.
	 * 
	 * @param globalOffset The offset of the mutation to load.
	 */
	void fetchMutation(long mutationOffset);
}
