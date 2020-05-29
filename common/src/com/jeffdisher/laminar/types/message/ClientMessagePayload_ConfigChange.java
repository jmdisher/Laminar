package com.jeffdisher.laminar.types.message;

import java.nio.ByteBuffer;

import com.jeffdisher.laminar.types.ClusterConfig;


/**
 * The CONFIG_CHANGE message only contains a ClusterConfig object.
 */
public class ClientMessagePayload_ConfigChange implements IClientMessagePayload {
	public static ClientMessagePayload_ConfigChange create(ClusterConfig config) {
		return new ClientMessagePayload_ConfigChange(config);
	}

	public static ClientMessagePayload_ConfigChange deserialize(ByteBuffer serialized) {
		ClusterConfig config = ClusterConfig.deserializeFrom(serialized);
		return new ClientMessagePayload_ConfigChange(config);
	}


	public final ClusterConfig config;
	
	private ClientMessagePayload_ConfigChange(ClusterConfig config) {
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
