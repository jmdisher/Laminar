package com.jeffdisher.laminar.network;

import java.nio.ByteBuffer;

import com.jeffdisher.laminar.types.ClusterConfig;


/**
 * The UPDATE_CONFIG message only contains a ClusterConfig object.
 */
public class ClientMessagePayload_UpdateConfig implements IClientMessagePayload {
	public static ClientMessagePayload_UpdateConfig create(ClusterConfig config) {
		return new ClientMessagePayload_UpdateConfig(config);
	}

	public static ClientMessagePayload_UpdateConfig deserialize(ByteBuffer serialized) {
		ClusterConfig config = ClusterConfig.deserializeFrom(serialized);
		return new ClientMessagePayload_UpdateConfig(config);
	}


	public final ClusterConfig config;
	
	private ClientMessagePayload_UpdateConfig(ClusterConfig config) {
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
