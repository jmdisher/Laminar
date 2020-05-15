package com.jeffdisher.laminar.types;

import java.nio.ByteBuffer;

import com.jeffdisher.laminar.utils.Assert;


/**
 * Information related to how a message was committed.
 * This will be the same across all nodes in the cluster for a given client message.  This is possible due to the fully
 * deterministic nature of how messages are interpreted.
 */
public class CommitInfo {
	public static CommitInfo create(Effect effect, long mutationOffset) {
		Assert.assertTrue(null != effect);
		// Mutation offsets are always positive
		Assert.assertTrue(mutationOffset > 0);
		return new CommitInfo(effect, mutationOffset);
	}

	public static CommitInfo deserialize(ByteBuffer serialized) {
		Effect effect = Effect.values()[Byte.toUnsignedInt(serialized.get())];
		long mutationOffset = serialized.getLong();
		return new CommitInfo(effect, mutationOffset);
	}


	/**
	 * The effect of executing the original message.
	 */
	public final Effect effect;
	/**
	 * The global mutation offset assigned to this message.
	 */
	public final long mutationOffset;

	private CommitInfo(Effect effect, long mutationOffset) {
		this.effect = effect;
		this.mutationOffset = mutationOffset;
	}

	public int serializedSize() {
		return Byte.BYTES + Long.BYTES;
	}

	public void serializeInto(ByteBuffer buffer) {
		buffer.put((byte)this.effect.ordinal());
		buffer.putLong(this.mutationOffset);
	}

	@Override
	public String toString() {
		return "(Effect=" + this.effect
				+ ", Mutation=" + this.mutationOffset
				+ ")";
	}


	public static enum Effect {
		/**
		 * There was an error in executing the commit.  In non-programmable topics, this typically means trying to write
		 * to a non-existent topic.
		 * Such messages CANNOT produce events.
		 */
		ERROR,
		/**
		 * Execution of the commit was valid.
		 * Such messages CAN produce events.
		 */
		VALID,
		/**
		 * The commit action was somehow invalid.  In non-programmable topics, this typically means trying to create a
		 * topic which already existed.
		 * Such messages CANNOT produce events.
		 */
		INVALID,
	}
}
