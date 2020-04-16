package com.jeffdisher.laminar.network;

import org.junit.Assert;
import org.junit.jupiter.api.Test;


/**
 * Tests around serialization and deserialization of ClientResponse objects.
 */
class TestClientResponse {
	@Test
	void testError() throws Throwable {
		long nonce = 1000;
		long lastCommitGlobalOffset = 1L;
		ClientResponse input = ClientResponse.error(nonce, lastCommitGlobalOffset);
		byte[] serialized = input.serialize();
		Assert.assertEquals(Byte.BYTES + Long.BYTES + Long.BYTES, serialized.length);
		ClientResponse output = ClientResponse.deserialize(serialized);
		Assert.assertEquals(input.type, output.type);
		Assert.assertEquals(input.nonce, output.nonce);
		Assert.assertEquals(input.lastCommitGlobalOffset, output.lastCommitGlobalOffset);
	}

	@Test
	void testReceived() throws Throwable {
		long nonce = 1000;
		long lastCommitGlobalOffset = 1L;
		ClientResponse input = ClientResponse.received(nonce, lastCommitGlobalOffset);
		byte[] serialized = input.serialize();
		Assert.assertEquals(Byte.BYTES + Long.BYTES + Long.BYTES, serialized.length);
		ClientResponse output = ClientResponse.deserialize(serialized);
		Assert.assertEquals(input.type, output.type);
		Assert.assertEquals(input.nonce, output.nonce);
		Assert.assertEquals(input.lastCommitGlobalOffset, output.lastCommitGlobalOffset);
	}

	@Test
	void testCommitted() throws Throwable {
		long nonce = 1000;
		long lastCommitGlobalOffset = 1L;
		ClientResponse input = ClientResponse.committed(nonce, lastCommitGlobalOffset);
		byte[] serialized = input.serialize();
		Assert.assertEquals(Byte.BYTES + Long.BYTES + Long.BYTES, serialized.length);
		ClientResponse output = ClientResponse.deserialize(serialized);
		Assert.assertEquals(input.type, output.type);
		Assert.assertEquals(input.nonce, output.nonce);
		Assert.assertEquals(input.lastCommitGlobalOffset, output.lastCommitGlobalOffset);
	}
}
