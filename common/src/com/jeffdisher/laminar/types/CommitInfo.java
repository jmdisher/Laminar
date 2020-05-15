package com.jeffdisher.laminar.types;

import java.nio.ByteBuffer;

import com.jeffdisher.laminar.utils.Assert;


/**
 * Information related to how a message was committed.
 * This will be the same across all nodes in the cluster for a given client message.  This is possible due to the fully
 * deterministic nature of how messages are interpreted.
 */
public class CommitInfo {
	public static CommitInfo create(long mutationOffset) {
		// Mutation offsets are always positive
		Assert.assertTrue(mutationOffset > 0);
		return new CommitInfo(mutationOffset);
	}

	public static CommitInfo deserialize(ByteBuffer serialized) {
		long mutationOffset = serialized.getLong();
		return new CommitInfo(mutationOffset);
	}


	/**
	 * The global mutation offset assigned to this message.
	 */
	public final long mutationOffset;

	private CommitInfo(long mutationOffset) {
		this.mutationOffset = mutationOffset;
	}

	public int serializedSize() {
		return Long.BYTES;
	}

	public void serializeInto(ByteBuffer buffer) {
		buffer.putLong(this.mutationOffset);
	}

	@Override
	public String toString() {
		return "(Mutation=" + this.mutationOffset + ")";
	}
}
