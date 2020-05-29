package com.jeffdisher.laminar.types.payload;

import java.nio.ByteBuffer;

import com.jeffdisher.laminar.types.ClusterConfig;


/**
 * Contains:
 * -config (ClusterConfig)
 */
public class Payload_Config implements IPayload {
	public static Payload_Config create(ClusterConfig config) {
		return new Payload_Config(config);
	}

	public static Payload_Config deserialize(ByteBuffer serialized) {
		ClusterConfig config = ClusterConfig.deserializeFrom(serialized);
		return new Payload_Config(config);
	}


	public final ClusterConfig config;
	
	private Payload_Config(ClusterConfig config) {
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
