package com.jeffdisher.laminar.types;

import java.nio.ByteBuffer;

import org.junit.Assert;
import org.junit.Test;


/**
 * Tests around the rules/restrictions and serialization/deserialization of TopicName objects.
 */
public class TestTopicName {
	@Test
	public void testSerializedSize() throws Throwable {
		TopicName topic = TopicName.fromString("test");
		Assert.assertEquals(5L, topic.serializedSize());
	}

	@Test
	public void testDeserialization() throws Throwable {
		TopicName topic = TopicName.fromString("test");
		ByteBuffer buffer = ByteBuffer.allocate(topic.serializedSize());
		topic.serializeInto(buffer);
		buffer.flip();
		TopicName result = TopicName.deserializeFrom(buffer);
		Assert.assertEquals(topic, result);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testInvalidCharset() throws Throwable {
		TopicName.fromString("test字");
	}

	@Test(expected = IllegalArgumentException.class)
	public void testInvalidChars() throws Throwable {
		TopicName.fromString("test&");
	}

	@Test(expected = IllegalArgumentException.class)
	public void testInvalidLength() throws Throwable {
		String foo = "";
		for (int i = 0; i < 256; ++i) {
			foo += "A";
		}
		TopicName.fromString(foo);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testEmpty() throws Throwable {
		TopicName.fromString("");
	}
}
