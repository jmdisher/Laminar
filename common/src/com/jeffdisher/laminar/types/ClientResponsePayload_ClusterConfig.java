package com.jeffdisher.laminar.types;

import java.nio.ByteBuffer;


/**
 * Used for ClientResponses which only contain a ClusterConfig.
 */
public class ClientResponsePayload_ClusterConfig implements IClientResponsePayload {
	public static ClientResponsePayload_ClusterConfig create(ClusterConfig config) {
		return new ClientResponsePayload_ClusterConfig(config);
	}

	public static ClientResponsePayload_ClusterConfig deserialize(ByteBuffer serialized) {
		ClusterConfig config = ClusterConfig.deserializeFrom(serialized);
		return new ClientResponsePayload_ClusterConfig(config);
	}


	public final ClusterConfig config;

	private ClientResponsePayload_ClusterConfig(ClusterConfig config) {
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
