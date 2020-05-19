package com.jeffdisher.laminar.utils;

import java.nio.ByteBuffer;


/**
 * A collection of common helper routines which don't have a more specific place to live.
 */
public class MiscHelpers {
	/**
	 * Reads byte[] from a buffer which was serialized with a preceding 2-byte size.
	 * Has the consequence of advancing the cursor through serialized.
	 * 
	 * @param serialized The buffer to read.
	 * @return The byte[].
	 */
	public static byte[] readSizedBytes(ByteBuffer serialized) {
		int size = Short.toUnsignedInt(serialized.getShort());
		byte[] buffer = new byte[size];
		serialized.get(buffer);
		return buffer;
	}

	/**
	 * Writes the given byte[] to the buffer after encoding a 2-byte size.
	 * Has the consequence of advancing the cursor through serialized.
	 * 
	 * @param buffer The buffer to write into.
	 * @param toWrite The bytes to write.
	 */
	public static void writeSizedBytes(ByteBuffer buffer, byte[] toWrite) {
		buffer.putShort((short)toWrite.length);
		buffer.put(toWrite);
	}

}
