package com.jeffdisher.laminar.contracts;

import avm.Blockchain;


/**
 * A simple parallel work unit manager.  Clients can use this to split out work units of a parallel task to be run by
 * individual clients.
 * Ignores the given key and looks only at the value, interpreting it as a big- endian 32-bit integer.
 * If this number is the next one expected (starts at 1), then it increments the next expected value, generates an
 * consequence, and returns success.  Otherwise, returns failure.
 */
public class WorkUnitManager {
	private static int _nextWorkUnit = 1;

	public static byte[] main() {
		byte[] value = Blockchain.getStorage(new byte[32]);
		Blockchain.require(Integer.BYTES == value.length);
		int proposedValue = _readInt(value);
		
		// Check if this was expected, generate the consequence (with the current value), and increment the counter.
		Blockchain.require(_nextWorkUnit == proposedValue);
		Blockchain.putStorage(new byte[32], value);
		_nextWorkUnit += 1;
		return new byte[0];
	}

	public static int _readInt(byte[] buffer) {
		return (  (0xff000000 & (buffer[0] << 24))
				| (0x00ff0000 & (buffer[1] << 16))
				| (0x0000ff00 & (buffer[2] << 8))
				| (0x000000ff & buffer[3])
		);
	}
}
