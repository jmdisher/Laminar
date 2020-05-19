package com.jeffdisher.laminar.types.event;

import java.nio.ByteBuffer;


/**
 * The TEMP message only contains a nameless payload.
 */
public class EventRecordPayload_Temp implements IEventRecordPayload {
	public static EventRecordPayload_Temp create(byte[] message) {
		return new EventRecordPayload_Temp(message);
	}

	public static EventRecordPayload_Temp deserialize(ByteBuffer serialized) {
		int size = Short.toUnsignedInt(serialized.getShort());
		byte[] buffer = new byte[size];
		serialized.get(buffer);
		return new EventRecordPayload_Temp(buffer);
	}


	public final byte[] contents;
	
	private EventRecordPayload_Temp(byte[] contents) {
		this.contents = contents;
	}

	@Override
	public int serializedSize() {
		return Short.BYTES + this.contents.length;
	}

	@Override
	public void serializeInto(ByteBuffer buffer) {
		buffer.putShort((short)this.contents.length);
		buffer.put(this.contents);
	}
}
