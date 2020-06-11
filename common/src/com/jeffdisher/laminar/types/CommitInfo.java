package com.jeffdisher.laminar.types;

import java.nio.ByteBuffer;

import com.jeffdisher.laminar.utils.Assert;


/**
 * Information related to how a message was committed.
 * This will be the same across all nodes in the cluster for a given client message.  This is possible due to the fully
 * deterministic nature of how messages are interpreted.
 */
public class CommitInfo {
	public static CommitInfo create(Effect effect, long intentionOffset) {
		Assert.assertTrue(null != effect);
		// Intention offsets are always positive
		Assert.assertTrue(intentionOffset > 0);
		return new CommitInfo(effect, intentionOffset);
	}

	public static CommitInfo deserialize(ByteBuffer serialized) {
		Effect effect = Effect.values()[Byte.toUnsignedInt(serialized.get())];
		long intentionOffset = serialized.getLong();
		return new CommitInfo(effect, intentionOffset);
	}


	/**
	 * The effect of executing the original message.
	 */
	public final Effect effect;
	/**
	 * The global intention offset assigned to this message.
	 */
	public final long intentionOffset;

	private CommitInfo(Effect effect, long intentionOffset) {
		this.effect = effect;
		this.intentionOffset = intentionOffset;
	}

	public int serializedSize() {
		return Byte.BYTES + Long.BYTES;
	}

	public void serializeInto(ByteBuffer buffer) {
		buffer.put((byte)this.effect.ordinal());
		buffer.putLong(this.intentionOffset);
	}

	@Override
	public String toString() {
		return "(Effect=" + this.effect
				+ ", Intention=" + this.intentionOffset
				+ ")";
	}


	public static enum Effect {
		/**
		 * There was an error in executing the commit.  In non-programmable topics, this typically means trying to write
		 * to a non-existent topic.
		 * Such messages CANNOT produce consequences.
		 */
		ERROR,
		/**
		 * Execution of the commit was valid.
		 * Such messages CAN produce consequences.
		 */
		VALID,
		/**
		 * The commit action was somehow invalid.  In non-programmable topics, this typically means trying to create a
		 * topic which already existed.
		 * Such messages CANNOT produce consequences.
		 */
		INVALID,
	}
}
