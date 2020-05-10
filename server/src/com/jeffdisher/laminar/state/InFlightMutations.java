package com.jeffdisher.laminar.state;

import java.util.LinkedList;

import com.jeffdisher.laminar.types.MutationRecord;
import com.jeffdisher.laminar.utils.Assert;


/**
 * Tracks the Mutations which haven't yet committed as well as information relating to where they are in the overall
 * mutation stream.
 * Note that this utility assumes it is being used strictly correctly so it doesn't explicitly handle bounds checks,
 * etc.  This means that indexing exceptions will be thrown if it is asked to provide data it was never given.
 */
public class InFlightMutations {
	// Tracking of in-flight mutations ready to be committed when the cluster agrees.
	// These must be committed in-order, so they are a queue with a base offset bias.
	// Note that we use a LinkedList since we want this to be addressable but also implement Queue.
	private LinkedList<MutationRecord> _inFlightMutations;
	private long _inFlightMutationOffsetBias;

	public InFlightMutations() {
		// The first mutation has offset 1L so we use that as the initial bias.
		_inFlightMutations = new LinkedList<>();
		_inFlightMutationOffsetBias = 1L;
	}

	/**
	 * @return The offset of the next mutation to be added or otherwise handled.
	 */
	public long getNextMutationOffset() {
		return _getNextMutationOffset();
	}

	/**
	 * @return True if there are no in-flight mutations (used in some checks to make sure commits can proceed).
	 */
	public boolean isEmpty() {
		return _inFlightMutations.isEmpty();
	}

	/**
	 * Adds a mutation to the in-flight list.  Asserts that this mutation has the expected offset.
	 * 
	 * @param mutation The mutation to add.
	 */
	public void add(MutationRecord mutation) {
		// Make sure that our offsets are consistent.
		Assert.assertTrue(_getNextMutationOffset() == mutation.globalOffset);
		_inFlightMutations.add(mutation);
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
	public boolean canCommitUpToMutation(long mutationOffset, long currentTermNumber) {
		boolean canCommit = false;
		for (MutationRecord mutation : _inFlightMutations) {
			if (mutation.globalOffset <= mutationOffset) {
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
	public MutationRecord removeFirstElementLessThanOrEqualTo(long mutationOffset) {
		MutationRecord removed = null;
		if ((_inFlightMutationOffsetBias <= mutationOffset) && !_inFlightMutations.isEmpty()) {
			removed = _inFlightMutations.remove();
			_inFlightMutationOffsetBias += 1;
			Assert.assertTrue(removed.globalOffset <= mutationOffset);
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
	public MutationRecord removeLastElementGreaterThanOrEqualTo(long mutationOffset) {
		MutationRecord removed = null;
		if (!_inFlightMutations.isEmpty() && (_inFlightMutations.getLast().globalOffset >= mutationOffset)) {
			removed = _inFlightMutations.removeLast();
			Assert.assertTrue(removed.globalOffset >= mutationOffset);
		}
		return removed;
	}

	/**
	 * Gets, but does NOT remove, the mutation with the given mutationOffset.
	 * 
	 * @param mutationOffset The offset of the mutation to return.
	 * @return The mutation with the given offset, null if there isn't one.
	 */
	public MutationRecord getMutationAtOffset(long mutationOffset) {
		MutationRecord mutation = null;
		if (mutationOffset >= _inFlightMutationOffsetBias) {
			int index = (int)(mutationOffset - _inFlightMutationOffsetBias);
			mutation = _inFlightMutations.get(index);
			Assert.assertTrue(mutationOffset == mutation.globalOffset);
		}
		return mutation;
	}

	/**
	 * Updates the base mutation offset for future in-flight commits to the one given.
	 * Asserts that this is only a single-step increment and that there aren't currently any in-flight mutations (since
	 * this would shift them from their true offsets).
	 * 
	 * @param mutationOffset The new mutation offset bias.
	 */
	public void updateBiasForDirectCommit(long mutationOffset) {
		Assert.assertTrue(_getNextMutationOffset() == mutationOffset);
		Assert.assertTrue(_inFlightMutations.isEmpty());
		_inFlightMutationOffsetBias += 1;
	}

	/**
	 * @return The term number of the last mutation in-flight.
	 */
	public long getLastTermNumber() {
		return _inFlightMutations.getLast().termNumber;
	}


	private long _getNextMutationOffset() {
		return _inFlightMutationOffsetBias + _inFlightMutations.size();
	}
}
