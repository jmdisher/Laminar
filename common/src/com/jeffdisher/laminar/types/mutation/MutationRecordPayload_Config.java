package com.jeffdisher.laminar.types.mutation;

import java.nio.ByteBuffer;

import com.jeffdisher.laminar.types.ClusterConfig;


/**
 * Used for the UPDATE_CONFIG mutation.
 */
public class MutationRecordPayload_Config implements IMutationRecordPayload {
	public static MutationRecordPayload_Config create(ClusterConfig config) {
		return new MutationRecordPayload_Config(config);
	}

	public static MutationRecordPayload_Config deserialize(ByteBuffer serialized) {
		ClusterConfig config = ClusterConfig.deserializeFrom(serialized);
		return new MutationRecordPayload_Config(config);
	}


	public final ClusterConfig config;
	
	private MutationRecordPayload_Config(ClusterConfig config) {
		this.config = config;
	}

	@Override
	public int serializedSize() {
		return this.config.serializedSize();
	}

	@Override
	public void serializeInto(ByteBuffer buffer) {
		this.config.serializeInto(buffer);
	}
}
