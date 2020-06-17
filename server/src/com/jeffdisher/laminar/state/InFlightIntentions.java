package com.jeffdisher.laminar.state;

import java.util.LinkedList;

import com.jeffdisher.laminar.types.Intention;
import com.jeffdisher.laminar.utils.Assert;


/**
 * Tracks the Intentions which haven't yet committed as well as information relating to where they are in the overall
 * intention stream.
 * Note that this utility assumes it is being used strictly correctly so it doesn't explicitly handle bounds checks,
 * etc.  This means that indexing exceptions will be thrown if it is asked to provide data it was never given.
 */
public class InFlightIntentions {
	// Tracking of in-flight intentions ready to be committed when the cluster agrees.
	// These must be committed in-order, so they are a queue with a base offset bias.
	// Note that we use a LinkedList since we want this to be addressable but also implement Queue.
	private LinkedList<Intention> _inFlightIntentions;
	private long _inFlightIntentionOffsetBias;

	public InFlightIntentions() {
		// The first mutation has offset 1L so we use that as the initial bias.
		_inFlightIntentions = new LinkedList<>();
		_inFlightIntentionOffsetBias = 1L;
	}

	/**
	 * Called by the NodeState to restore the state of the receiver after a restart (not called on a normal start).
	 * This is called before the system finishes starting up so nothing else is in-flight.
	 * 
	 * @param lastCommittedIntentionOffset The offset of the last committed intention.
	 */
	public void restoreState(long lastCommittedIntentionOffset) {
		_inFlightIntentionOffsetBias = lastCommittedIntentionOffset + 1L;
	}

	/**
	 * @return The offset of the next mutation to be added or otherwise handled.
	 */
	public long getNextIntentionOffset() {
		return _getNextIntentionOffset();
	}

	/**
	 * @return True if there are no in-flight mutations (used in some checks to make sure commits can proceed).
	 */
	public boolean isEmpty() {
		return _inFlightIntentions.isEmpty();
	}

	/**
	 * Adds a mutation to the in-flight list.  Asserts that this mutation has the expected offset.
	 * 
	 * @param mutation The mutation to add.
	 */
	public void add(Intention mutation) {
		// Make sure that our offsets are consistent.
		Assert.assertTrue(_getNextIntentionOffset() == mutation.intentionOffset);
		_inFlightIntentions.add(mutation);
	}

	/**
	 * Used in part of the handling of how to commit after an election (section 5.4.2 of the Raft paper):  A mutation
	 * cannot be committed unless it was created in the current term (that is, by the current leader) or is followed by
	 * a mutation created in the current term which is also ready to commit.
	 * 
	 * @param mutationOffset The offset of the latest mutation which we currently believe we can commit.
	 * @param currentTermNumber The term number where the current leader was elected.
	 * @return True if the mutations up to and including mutationOffset can be committed.
	 */
	public boolean canCommitUpToIntention(long mutationOffset, long currentTermNumber) {
		boolean canCommit = false;
		for (Intention mutation : _inFlightIntentions) {
			if (mutation.intentionOffset <= mutationOffset) {
				if (currentTermNumber == mutation.termNumber) {
					// We found a match for this term, so all previous mutations can be committed.
					canCommit = true;
					break;
				}
			} else {
				// We reached the end of the extent which can currently be committed and didn't find our current term.
				break;
			}
		}
		return canCommit;
	}

	/**
	 * Removes and returns the first mutation in the list which has a mutation offset of at most the given
	 * mutationOffset.  Note that the internal mutation offset bias will also be updated to account for the removal.
	 * 
	 * @param mutationOffset The upper limit offset of the mutation to remove.
	 * @return The mutation, or null if all the mutations are later or there are no mutations.
	 */
	public Intention removeFirstElementLessThanOrEqualTo(long mutationOffset) {
		Intention removed = null;
		if ((_inFlightIntentionOffsetBias <= mutationOffset) && !_inFlightIntentions.isEmpty()) {
			removed = _inFlightIntentions.remove();
			_inFlightIntentionOffsetBias += 1;
			Assert.assertTrue(removed.intentionOffset <= mutationOffset);
		}
		return removed;
	}

	/**
	 * Removes and returns the last mutation in the list which has a mutation offset of at least the given
	 * mutationOffset.
	 * 
	 * @param mutationOffset The lower limit offset of the mutation to remove.
	 * @return The mutation, or null if all the mutations are earlier or there are no mutations.
	 */
	public Intention removeLastElementGreaterThanOrEqualTo(long mutationOffset) {
		Intention removed = null;
		if (!_inFlightIntentions.isEmpty() && (_inFlightIntentions.getLast().intentionOffset >= mutationOffset)) {
			removed = _inFlightIntentions.removeLast();
			Assert.assertTrue(removed.intentionOffset >= mutationOffset);
		}
		return removed;
	}

	/**
	 * Gets, but does NOT remove, the mutation with the given mutationOffset.
	 * 
	 * @param mutationOffset The offset of the mutation to return.
	 * @return The intention with the given offset, null if there isn't one.
	 */
	public Intention getMutationAtOffset(long mutationOffset) {
		Intention mutation = null;
		if (mutationOffset >= _inFlightIntentionOffsetBias) {
			int index = (int)(mutationOffset - _inFlightIntentionOffsetBias);
			mutation = _inFlightIntentions.get(index);
			Assert.assertTrue(mutationOffset == mutation.intentionOffset);
		}
		return mutation;
	}

	/**
	 * Updates the base intention offset for future in-flight commits to the one given.
	 * Asserts that this is only a single-step increment and that there aren't currently any in-flight intentions (since
	 * this would shift them from their true offsets).
	 * 
	 * @param mutationOffset The new intention offset bias.
	 */
	public void updateBiasForDirectCommit(long mutationOffset) {
		Assert.assertTrue(_getNextIntentionOffset() == mutationOffset);
		Assert.assertTrue(_inFlightIntentions.isEmpty());
		_inFlightIntentionOffsetBias += 1;
	}

	/**
	 * @return The term number of the last intention in-flight.
	 */
	public long getLastTermNumber() {
		return _inFlightIntentions.getLast().termNumber;
	}


	private long _getNextIntentionOffset() {
		return _inFlightIntentionOffsetBias + _inFlightIntentions.size();
	}
}
