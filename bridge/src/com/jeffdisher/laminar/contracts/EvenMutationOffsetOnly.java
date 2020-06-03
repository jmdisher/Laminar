package com.jeffdisher.laminar.contracts;

import avm.Blockchain;


/**
 * This contract will pass all PUT and DELETE calls through to events but only when the mutation offset is even.
 * Otherwise, it will fail.
 */
public class EvenMutationOffsetOnly {
	private static long _previousMutationOffset;

	public static byte[] main() {
		// Just verify that our invariant is true.
		Blockchain.require(0L == (_previousMutationOffset % 2L));
		// We capture this here to make sure that the graph doesn't write-back, on failure.
		_previousMutationOffset = Blockchain.getBlockNumber();
		// Similarly, create an event which should be dropped on the failure.
		if (0L != (Blockchain.getBlockNumber() % 2L)) {
			// (this should NOT be observable under any circumstances).
			Blockchain.putStorage(new byte[32], new byte[] {42});
		}
		// Post the event (this will work on PUT and DELETE but will be reverted if the final require fails).
		byte[] key = Blockchain.getData();
		byte[] value = Blockchain.getStorage(key);
		Blockchain.putStorage(key, value);
		
		// We will fail and revert everything if the mutation offset isn't an even number.
		Blockchain.require(0L == (Blockchain.getBlockNumber() % 2L));
		return new byte[0];
	}
}
