package com.jeffdisher.laminar.disk;

import java.util.function.Consumer;

import com.jeffdisher.laminar.state.StateSnapshot;
import com.jeffdisher.laminar.types.Consequence;
import com.jeffdisher.laminar.types.TopicName;


/**
 * Callbacks sent by the DiskManager, on its thread (implementor will need to hand these off to a different thread).
 */
public interface IDiskManagerBackgroundCallbacks {
	void ioEnqueueDiskCommandForMainThread(Consumer<StateSnapshot> command);

	/**
	 * A previously requested mutation commit operation has completed.
	 * 
	 * @param completed The record which has now committed.
	 */
	void mainMutationWasCommitted(CommittedMutationRecord completed);

	/**
	 * A previously requested consequence commit operation has completed.
	 * 
	 * @param topic The topic to which the consequence was committed.
	 * @param completed The record which has now committed.
	 */
	void mainConsequenceWasCommitted(TopicName topic, Consequence completed);

	/**
	 * A previously requested mutation record has been fetched.
	 * 
	 * @param snapshot The state created when this event started.
	 * @param previousMutationTermNumber The term number of the mutation before this one (0 if it is the first mutation).
	 * @param record The record which was fetched from storage.
	 */
	void mainMutationWasFetched(StateSnapshot snapshot, long previousMutationTermNumber, CommittedMutationRecord record);

	/**
	 * A previously requested consequence record has been fetched.
	 * 
	 * @param topic The topic from which the consequence was fetched.
	 * @param record The record which was fetched from storage.
	 */
	void mainConsequenceWasFetched(TopicName topic, Consequence record);
}
