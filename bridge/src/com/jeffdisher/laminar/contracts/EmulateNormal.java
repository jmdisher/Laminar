package com.jeffdisher.laminar.contracts;

import avm.Blockchain;


/**
 * A test contract which emulates the normal behaviour of PUT/DELETE messages.  It just generates the normal consequence
 * which would be created by either message.
 * This exists just to test the overhead of calling into the AVM versus running inline.
 */
public class EmulateNormal {
	public static byte[] main() {
		// Emulate these, directly.
		byte[] key = Blockchain.getData();
		// (AVM requires that we use a 32-byte key).
		byte[] value = Blockchain.getStorage(new byte[32]);
		Blockchain.putStorage(key, value);
		return new byte[0];
	}
}
