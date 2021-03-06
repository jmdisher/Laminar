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
	 * A previously requested intention commit operation has completed.
	 * 
	 * @param completed The record which has now committed.
	 */
	void mainIntentionWasCommitted(CommittedIntention completed);

	/**
	 * A previously requested consequence commit operation has completed.
	 * 
	 * @param topic The topic to which the consequence was committed.
	 * @param completed The record which has now committed.
	 */
	void mainConsequenceWasCommitted(TopicName topic, Consequence completed);

	/**
	 * A previously requested intention record has been fetched.
	 * 
	 * @param snapshot The state created when this event started.
	 * @param previousIntentionTermNumber The term number of the intention before this one (0 if it is the first intention).
	 * @param record The record which was fetched from storage.
	 */
	void mainIntentionWasFetched(StateSnapshot snapshot, long previousIntentionTermNumber, CommittedIntention record);

	/**
	 * A previously requested consequence record has been fetched.
	 * 
	 * @param topic The topic from which the consequence was fetched.
	 * @param record The record which was fetched from storage.
	 */
	void mainConsequenceWasFetched(TopicName topic, Consequence record);
}
