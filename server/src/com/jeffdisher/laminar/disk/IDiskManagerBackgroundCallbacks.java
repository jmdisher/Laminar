package com.jeffdisher.laminar.disk;

import java.util.function.Consumer;

import com.jeffdisher.laminar.state.StateSnapshot;
import com.jeffdisher.laminar.types.EventRecord;
import com.jeffdisher.laminar.types.MutationRecord;
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
	void mainMutationWasCommitted(MutationRecord completed);

	/**
	 * A previously requested event commit operation has completed.
	 * 
	 * @param topic The topic to which the event was committed.
	 * @param completed The record which has now committed.
	 */
	void mainEventWasCommitted(TopicName topic, EventRecord completed);

	/**
	 * A previously requested mutation record has been fetched.
	 * 
	 * @param snapshot The state created when this event started.
	 * @param previousMutationTermNumber The term number of the mutation before this one (0 if it is the first mutation).
	 * @param record The record which was fetched from storage.
	 */
	void mainMutationWasFetched(StateSnapshot snapshot, long previousMutationTermNumber, MutationRecord record);

	/**
	 * A previously requested event record has been fetched.
	 * 
	 * @param topic The topic from which the event was fetched.
	 * @param record The record which was fetched from storage.
	 */
	void mainEventWasFetched(TopicName topic, EventRecord record);
}
