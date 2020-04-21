package com.jeffdisher.laminar.types;

import java.util.UUID;

import org.junit.Assert;
import org.junit.Test;


/**
 * Tests around serialization and deserialization of MutationRecord objects.
 */
public class TestMutationRecord {
	@Test
	public void testBasic() throws Throwable {
		long globalOffset = 1L;
		UUID clientId = UUID.randomUUID();
		long clientNonce = 1L;
		byte[] payload = new byte[] { 1, 2, 3 };
		MutationRecord record = MutationRecord.generateRecord(MutationRecordType.TEMP, globalOffset, clientId, clientNonce, payload);
		byte[] serialized = record.serialize();
		MutationRecord deserialized = MutationRecord.deserialize(serialized);
		Assert.assertEquals(record.type, deserialized.type);
		Assert.assertEquals(record.globalOffset, deserialized.globalOffset);
		Assert.assertEquals(record.clientId, deserialized.clientId);
		Assert.assertEquals(record.clientNonce, deserialized.clientNonce);
		Assert.assertArrayEquals(record.payload, deserialized.payload);
	}
}
