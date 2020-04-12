package com.jeffdisher.laminar.types;

import java.util.UUID;

import org.junit.Assert;
import org.junit.jupiter.api.Test;


/**
 * Tests around serialization and deserialization of EventRecord objects.
 */
class TestEventRecord {
	@Test
	void testBasic() throws Throwable {
		long globalOffset = 1L;
		long localOffset = 1L;
		UUID clientId = UUID.randomUUID();
		byte[] payload = new byte[] { 1, 2, 3 };
		EventRecord record = EventRecord.generateRecord(globalOffset, localOffset, clientId, payload);
		byte[] serialized = record.serialize();
		EventRecord deserialized = EventRecord.deserialize(serialized);
		Assert.assertEquals(record.globalOffset, deserialized.globalOffset);
		Assert.assertEquals(record.localOffset, deserialized.localOffset);
		Assert.assertEquals(record.clientId, deserialized.clientId);
		Assert.assertArrayEquals(record.payload, deserialized.payload);
	}
}
