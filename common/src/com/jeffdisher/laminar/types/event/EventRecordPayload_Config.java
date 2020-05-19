package com.jeffdisher.laminar.types.event;

import java.nio.ByteBuffer;

import com.jeffdisher.laminar.types.ClusterConfig;


/**
 * Used for the CHANGE_CONFIG synthetic EventRecord.
 */
public class EventRecordPayload_Config implements IEventRecordPayload {
	public static EventRecordPayload_Config create(ClusterConfig config) {
		return new EventRecordPayload_Config(config);
	}

	public static EventRecordPayload_Config deserialize(ByteBuffer serialized) {
		ClusterConfig config = ClusterConfig.deserializeFrom(serialized);
		return new EventRecordPayload_Config(config);
	}


	public final ClusterConfig config;
	
	private EventRecordPayload_Config(ClusterConfig config) {
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
