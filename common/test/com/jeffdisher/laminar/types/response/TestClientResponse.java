package com.jeffdisher.laminar.types.response;

import org.junit.Assert;
import org.junit.Test;

import com.jeffdisher.laminar.types.CommitInfo;
import com.jeffdisher.laminar.types.response.ClientResponsePayload_Commit;


/**
 * Tests around serialization and deserialization of ClientResponse objects.
 */
public class TestClientResponse {
	@Test
	public void testError() throws Throwable {
		long nonce = 1000;
		long lastCommitGlobalOffset = 1L;
		ClientResponse input = ClientResponse.error(nonce, lastCommitGlobalOffset);
		byte[] serialized = input.serialize();
		Assert.assertEquals(Byte.BYTES + Long.BYTES + Long.BYTES, serialized.length);
		ClientResponse output = ClientResponse.deserialize(serialized);
		Assert.assertEquals(input.type, output.type);
		Assert.assertEquals(input.nonce, output.nonce);
		Assert.assertEquals(input.lastCommitIntentionOffset, output.lastCommitIntentionOffset);
	}

	@Test
	public void testReceived() throws Throwable {
		long nonce = 1000;
		long lastCommitGlobalOffset = 1L;
		ClientResponse input = ClientResponse.received(nonce, lastCommitGlobalOffset);
		byte[] serialized = input.serialize();
		Assert.assertEquals(Byte.BYTES + Long.BYTES + Long.BYTES, serialized.length);
		ClientResponse output = ClientResponse.deserialize(serialized);
		Assert.assertEquals(input.type, output.type);
		Assert.assertEquals(input.nonce, output.nonce);
		Assert.assertEquals(input.lastCommitIntentionOffset, output.lastCommitIntentionOffset);
	}

	@Test
	public void testCommitted() throws Throwable {
		long nonce = 1000;
		long thisCommitOffset = 1L;
		long lastCommitGlobalOffset = 2L;
		ClientResponse input = ClientResponse.committed(nonce, lastCommitGlobalOffset, CommitInfo.create(CommitInfo.Effect.VALID, thisCommitOffset));
		byte[] serialized = input.serialize();
		Assert.assertEquals(Byte.BYTES + Long.BYTES + Long.BYTES + Byte.BYTES + Long.BYTES, serialized.length);
		ClientResponse output = ClientResponse.deserialize(serialized);
		Assert.assertEquals(input.type, output.type);
		Assert.assertEquals(input.nonce, output.nonce);
		Assert.assertEquals(input.lastCommitIntentionOffset, output.lastCommitIntentionOffset);
		Assert.assertEquals(CommitInfo.Effect.VALID, ((ClientResponsePayload_Commit)output.payload).info.effect);
		Assert.assertEquals(thisCommitOffset, ((ClientResponsePayload_Commit)output.payload).info.intentionOffset);
	}
}
