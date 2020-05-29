package com.jeffdisher.laminar.types.event;

import java.nio.ByteBuffer;

import com.jeffdisher.laminar.utils.MiscHelpers;


/**
 * The CREATE message encodes code and arguments as byte[].
 */
public class EventRecordPayload_Create implements IEventRecordPayload {
	public static EventRecordPayload_Create create(byte[] code, byte[] arguments) {
		return new EventRecordPayload_Create(code, arguments);
	}

	public static EventRecordPayload_Create deserialize(ByteBuffer serialized) {
		byte[] code = MiscHelpers.readSizedBytes(serialized);
		byte[] arguments = MiscHelpers.readSizedBytes(serialized);
		return new EventRecordPayload_Create(code, arguments);
	}


	public final byte[] code;
	public final byte[] arguments;
	
	private EventRecordPayload_Create(byte[] code, byte[] arguments) {
		this.code = code;
		this.arguments = arguments;
	}

	@Override
	public int serializedSize() {
		return Short.BYTES
				+ this.code.length
				+ Short.BYTES
				+ this.arguments.length
		;
	}

	@Override
	public void serializeInto(ByteBuffer buffer) {
		MiscHelpers.writeSizedBytes(buffer, this.code);
		MiscHelpers.writeSizedBytes(buffer, this.arguments);
	}
}
