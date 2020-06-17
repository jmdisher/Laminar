package com.jeffdisher.laminar.disk;

import java.util.List;
import java.util.Set;

import com.jeffdisher.laminar.types.CommitInfo;
import com.jeffdisher.laminar.types.Consequence;
import com.jeffdisher.laminar.types.Intention;
import com.jeffdisher.laminar.types.TopicName;


/**
 * Interface of DiskManager to make unit testing NodeState easier.
 */
public interface IDiskManager {
	/**
	 * Called by the NodeState to restore the state of the receiver after a restart (not called on a normal start).
	 * This is called before the system finishes starting up so nothing else is in-flight.
	 * 
	 * @param activeTopics The set of all topics which should be opened as they haven't been deleted.
	 */
	void restoreState(Set<TopicName> activeTopics);

	/**
	 * Requests that the consequence with the associated localOffset offset be asynchronously fetched.
	 * 
	 * @param topic The topic where to search.
	 * @param localOffset The offset of the consequence to load.
	 */
	void fetchConsequence(TopicName topic, long localOffset);

	/**
	 * Requests that the intention with the associated global offset be asynchronously fetched.
	 * 
	 * @param globalOffset The offset of the intention to load.
	 */
	void fetchIntention(long intentionOffset);

	/**
	 * Requests that an intention, with the executed effect, and associated consequences (could be null if not a VALID
	 * effect) be written to disk.  The receiver will ensure that the write operations are ordered such that they can be
	 * considered an effectively atomic write.
	 * 
	 * @param intention The intention to commit.
	 * @param effect The effect of the execution of this intention.
	 * @param consequences The list of consequences associated with the execution of the intention (null iff effect was
	 * not VALID).
	 * @param newTransformedCode Non-null for TOPIC_CREATE, contains the transformed code (empty array if
	 * non-programmable).
	 * @param objectGraph The serialized object graph after executing the intention (null if the topic was
	 * non-programmable or the intention does cause execution).
	 */
	void commit(Intention intention, CommitInfo.Effect effect, List<Consequence> consequences, byte[] newTransformedCode, byte[] objectGraph);
}
