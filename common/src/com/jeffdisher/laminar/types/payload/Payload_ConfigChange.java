package com.jeffdisher.laminar.types.payload;

import java.nio.ByteBuffer;

import com.jeffdisher.laminar.types.ClusterConfig;


/**
 * Contains:
 * -config (ClusterConfig)
 */
public final class Payload_ConfigChange implements IPayload {
	public static Payload_ConfigChange create(ClusterConfig config) {
		return new Payload_ConfigChange(config);
	}

	public static Payload_ConfigChange deserialize(ByteBuffer serialized) {
		ClusterConfig config = ClusterConfig.deserializeFrom(serialized);
		return new Payload_ConfigChange(config);
	}


	public final ClusterConfig config;
	
	private Payload_ConfigChange(ClusterConfig config) {
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

	@Override
	public boolean equals(Object arg0) {
		boolean isEqual = (this == arg0);
		if (!isEqual && (null != arg0) && (this.getClass() == arg0.getClass())) {
			Payload_ConfigChange object = (Payload_ConfigChange) arg0;
			isEqual = this.config.equals(object.config);
		}
		return isEqual;
	}

	@Override
	public int hashCode() {
		return this.config.hashCode();
	}

	@Override
	public String toString() {
		return "Payload_ConfigChange(" + this.config + ")";
	}
}
