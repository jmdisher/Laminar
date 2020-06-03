package com.jeffdisher.laminar.contracts;

import avm.Blockchain;


public class SimpleDeployment {
	private static byte _nextValue = 0;

	static {
		byte[] key = Blockchain.getData();
		Blockchain.putStorage(key, new byte[] {_nextValue});
		_nextValue += 1;
	}

	public static byte[] main() {
		byte[] key = Blockchain.getData();
		Blockchain.putStorage(key, new byte[] {_nextValue});
		_nextValue += 1;
		return new byte[0];
	}
}
