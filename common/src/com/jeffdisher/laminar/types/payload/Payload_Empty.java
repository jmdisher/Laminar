package com.jeffdisher.laminar.types.payload;

import java.nio.ByteBuffer;


/**
 * Contains nothing - used for message types which require no payload.
 */
public final class Payload_Empty implements IPayload {
	public static Payload_Empty create() {
		return new Payload_Empty();
	}

	public static Payload_Empty deserialize(ByteBuffer serialized) {
		return new Payload_Empty();
	}


	private Payload_Empty() {
	}

	@Override
	public int serializedSize() {
		return 0;
	}

	@Override
	public void serializeInto(ByteBuffer buffer) {
	}

	@Override
	public boolean equals(Object arg0) {
		boolean isEqual = (this == arg0);
		if (!isEqual && (null != arg0) && (this.getClass() == arg0.getClass())) {
			isEqual = true;
		}
		return isEqual;
	}

	@Override
	public int hashCode() {
		return 0;
	}

	@Override
	public String toString() {
		return "Payload_Empty()";
	}
}
