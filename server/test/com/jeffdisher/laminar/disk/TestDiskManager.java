package com.jeffdisher.laminar.disk;

import java.io.File;
import java.io.FileInputStream;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.UUID;
import java.util.function.Consumer;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.jeffdisher.laminar.state.StateSnapshot;
import com.jeffdisher.laminar.types.CommitInfo;
import com.jeffdisher.laminar.types.Consequence;
import com.jeffdisher.laminar.types.Intention;
import com.jeffdisher.laminar.types.TopicName;


public class TestDiskManager {
	@Rule
	public TemporaryFolder _folder = new TemporaryFolder();

	@Test
	public void testStartStop() throws Throwable {
		LatchedCallbacks callbacks = new LatchedCallbacks();
		DiskManager manager = new DiskManager(_folder.newFolder(), callbacks);
		manager.startAndWaitForReady();
		manager.stopAndWaitForTermination();
	}

	/**
	 * Just write 2 records and fetch the first.
	 */
	@Test
	public void testSimpleWriteAndFetch() throws Throwable {
		TopicName topic = TopicName.fromString("fake");
		// Create these intentions just to call the helper correctly.
		Intention ignored1 = Intention.put(1L, 1L, topic, UUID.randomUUID(), 1L, new byte[0], new byte[] {1});
		Intention ignored2 = Intention.put(1L, 2L, topic, UUID.randomUUID(), 2L, new byte[0], new byte[] {1});
		Consequence event1 = Consequence.createTopic(1L, 1L, 1L, UUID.randomUUID(), 1L, new byte[0], new byte[] {1});
		Consequence event2 = Consequence.put(1L, 2L, 2L, UUID.randomUUID(), 2L, new byte[0], new byte[] {1});
		LatchedCallbacks callbacks = new LatchedCallbacks();
		DiskManager manager = new DiskManager(_folder.newFolder(), callbacks);
		manager.startAndWaitForReady();
		
		manager.commit(ignored1, CommitInfo.Effect.VALID, Collections.singletonList(event1), null, null);
		while (callbacks.commitEventCount < 1) { callbacks.runOneCommand(); }
		manager.commit(ignored2, CommitInfo.Effect.VALID, Collections.singletonList(event2), null, null);
		while (callbacks.commitEventCount < 2) { callbacks.runOneCommand(); }
		callbacks.expectedEvent = event2;
		manager.fetchConsequence(topic, 2L);
		while (callbacks.fetchEventCount < 1) { callbacks.runOneCommand(); }
		
		manager.stopAndWaitForTermination();
	}

	/**
	 * Write a few events and mutations and try to fetch them, independently.
	 */
	@Test
	public void testMutationAndEventCommitAndFetch() throws Throwable {
		TopicName topic = TopicName.fromString("fake");
		Intention mutation1 = Intention.put(1L, 1L, topic, UUID.randomUUID(), 1L, new byte[0], new byte[] {1});
		Intention mutation2 = Intention.put(1L, 2L, topic, UUID.randomUUID(), 2L, new byte[0], new byte[] {1});
		Consequence event1 = Consequence.createTopic(1L, 1L, 1L, UUID.randomUUID(), 1L, new byte[0], new byte[] {1});
		Consequence event2 = Consequence.put(1L, 2L, 2L, UUID.randomUUID(), 2L, new byte[0], new byte[] {1});
		
		LatchedCallbacks callbacks = new LatchedCallbacks();
		DiskManager manager = new DiskManager(_folder.newFolder(), callbacks);
		manager.startAndWaitForReady();
		
		manager.commit(mutation1, CommitInfo.Effect.VALID, Collections.singletonList(event1), null, null);
		while (callbacks.commitMutationCount < 1) { callbacks.runOneCommand(); }
		manager.commit(mutation2, CommitInfo.Effect.VALID, Collections.singletonList(event2), null, null);
		while (callbacks.commitMutationCount < 2) { callbacks.runOneCommand(); }
		
		callbacks.expectedMutation = mutation1;
		callbacks.expectedEvent = event2;
		
		manager.fetchIntention(1L);
		manager.fetchConsequence(topic, 2L);
		while (callbacks.fetchMutationCount < 1) { callbacks.runOneCommand(); }
		while (callbacks.fetchEventCount < 1) { callbacks.runOneCommand(); }
		
		manager.stopAndWaitForTermination();
	}

	/**
	 * Write a few intentions and verify that they look correct on-disk.
	 */
	@Test
	public void testWritingIntentions() throws Throwable {
		File directory = _folder.newFolder();
		TopicName topic = TopicName.fromString("fake");
		Intention intention1 = Intention.put(1L, 1L, topic, UUID.randomUUID(), 1L, new byte[0], new byte[] {1});
		Intention intention2 = Intention.put(1L, 2L, topic, UUID.randomUUID(), 2L, new byte[0], new byte[] {1});
		
		LatchedCallbacks callbacks = new LatchedCallbacks();
		DiskManager manager = new DiskManager(directory, callbacks);
		manager.startAndWaitForReady();
		
		manager.commit(intention1, CommitInfo.Effect.VALID, Collections.emptyList(), null, null);
		while (callbacks.commitMutationCount < 1) { callbacks.runOneCommand(); }
		manager.commit(intention2, CommitInfo.Effect.VALID, Collections.emptyList(), null, null);
		while (callbacks.commitMutationCount < 2) { callbacks.runOneCommand(); }
		manager.stopAndWaitForTermination();
		
		// Verify that the intention log file contains the expected data.
		int intention1Size = intention1.serializedSize();
		int intention2Size = intention2.serializedSize();
		File logFile = new File(new File(directory, DiskManager.INTENTION_DIRECTORY_NAME), LogFileDomain.LOG_FILE_NAME);
		long fileLength = logFile.length();
		Assert.assertEquals((2 * Short.BYTES) + (2 * Byte.BYTES) + intention1Size + intention2Size, (int)fileLength);
		int intention1FileOffset;
		int intention2FileOffset;
		try (FileInputStream stream = new FileInputStream(logFile)) {
			ByteBuffer buffer = ByteBuffer.allocate((int)fileLength);
			int read = stream.getChannel().read(buffer);
			Assert.assertEquals((int)fileLength, read);
			buffer.flip();
			
			intention1FileOffset = buffer.position();
			short size = buffer.getShort();
			Assert.assertEquals(Short.toUnsignedInt(size), intention1Size);
			Assert.assertEquals(CommitInfo.Effect.VALID, CommitInfo.Effect.values()[(int)buffer.get()]);
			byte[] temp = new byte[(int)size];
			buffer.get(temp);
			Assert.assertArrayEquals(intention1.serialize(), temp);
			
			intention2FileOffset = buffer.position();
			size = buffer.getShort();
			Assert.assertEquals(Short.toUnsignedInt(size), intention2Size);
			Assert.assertEquals(CommitInfo.Effect.VALID, CommitInfo.Effect.values()[(int)buffer.get()]);
			temp = new byte[(int)size];
			buffer.get(temp);
			Assert.assertArrayEquals(intention2.serialize(), temp);
		}
		
		// Verify that the intention index file contains the expected data.
		File indexFile = new File(new File(directory, DiskManager.INTENTION_DIRECTORY_NAME), LogFileDomain.INDEX_FILE_NAME);
		long indexFileLength = indexFile.length();
		Assert.assertEquals(2 * IndexEntry.BYTES, (int)indexFileLength);
		try (FileInputStream stream = new FileInputStream(indexFile)) {
			ByteBuffer buffer = ByteBuffer.allocate((int)indexFileLength);
			int read = stream.getChannel().read(buffer);
			Assert.assertEquals((int)indexFileLength, read);
			buffer.flip();
			
			IndexEntry entry1 = IndexEntry.read(buffer);
			Assert.assertEquals(intention1.intentionOffset, entry1.logicalOffset);
			Assert.assertEquals(intention1FileOffset, entry1.fileOffset);
			
			IndexEntry entry2 = IndexEntry.read(buffer);
			Assert.assertEquals(intention2.intentionOffset, entry2.logicalOffset);
			Assert.assertEquals(intention2FileOffset, entry2.fileOffset);
		}
	}

	/**
	 * Write a few consequences and verify that they look correct on-disk.
	 */
	@Test
	public void testWritingConsequences() throws Throwable {
		File directory = _folder.newFolder();
		TopicName topic = TopicName.fromString("fake");
		// Create these intentions just to call the helper correctly.
		Intention ignored1 = Intention.put(1L, 1L, topic, UUID.randomUUID(), 1L, new byte[0], new byte[] {1});
		Intention ignored2 = Intention.put(1L, 2L, topic, UUID.randomUUID(), 2L, new byte[0], new byte[] {1});
		Consequence consequence1 = Consequence.createTopic(1L, 1L, 1L, UUID.randomUUID(), 1L, new byte[0], new byte[] {1});
		Consequence consequence2 = Consequence.put(1L, 2L, 2L, UUID.randomUUID(), 2L, new byte[0], new byte[] {1});
		
		LatchedCallbacks callbacks = new LatchedCallbacks();
		DiskManager manager = new DiskManager(directory, callbacks);
		manager.startAndWaitForReady();
		
		manager.commit(ignored1, CommitInfo.Effect.VALID, Collections.singletonList(consequence1), null, null);
		while (callbacks.commitEventCount < 1) { callbacks.runOneCommand(); }
		manager.commit(ignored2, CommitInfo.Effect.VALID, Collections.singletonList(consequence2), null, null);
		while (callbacks.commitEventCount < 2) { callbacks.runOneCommand(); }
		manager.stopAndWaitForTermination();
		
		// Verify that the consequence log file contains the expected data.
		byte[] serialized1 = consequence1.serialize();
		byte[] serialized2 = consequence2.serialize();
		File logFile = new File(new File(new File(directory, DiskManager.CONSEQUENCE_TOPICS_DIRECTORY_NAME), topic.string), LogFileDomain.LOG_FILE_NAME);
		long fileLength = logFile.length();
		Assert.assertEquals((2 * Short.BYTES) + serialized1.length + serialized2.length, (int)fileLength);
		int consequence1FileOffset;
		int consequence2FileOffset;
		try (FileInputStream stream = new FileInputStream(logFile)) {
			ByteBuffer buffer = ByteBuffer.allocate((int)fileLength);
			int read = stream.getChannel().read(buffer);
			Assert.assertEquals((int)fileLength, read);
			buffer.flip();
			
			consequence1FileOffset = buffer.position();
			short size = buffer.getShort();
			Assert.assertEquals(Short.toUnsignedInt(size), serialized1.length);
			byte[] temp = new byte[(int)size];
			buffer.get(temp);
			Assert.assertArrayEquals(serialized1, temp);
			
			consequence2FileOffset = buffer.position();
			size = buffer.getShort();
			Assert.assertEquals(Short.toUnsignedInt(size), serialized2.length);
			temp = new byte[(int)size];
			buffer.get(temp);
			Assert.assertArrayEquals(serialized2, temp);
		}
		
		// Verify that the consequence index file contains the expected data.
		File indexFile = new File(new File(new File(directory, DiskManager.CONSEQUENCE_TOPICS_DIRECTORY_NAME), topic.string), LogFileDomain.INDEX_FILE_NAME);
		long indexFileLength = indexFile.length();
		Assert.assertEquals(2 * IndexEntry.BYTES, (int)indexFileLength);
		try (FileInputStream stream = new FileInputStream(indexFile)) {
			ByteBuffer buffer = ByteBuffer.allocate((int)indexFileLength);
			int read = stream.getChannel().read(buffer);
			Assert.assertEquals((int)indexFileLength, read);
			buffer.flip();
			
			IndexEntry entry1 = IndexEntry.read(buffer);
			Assert.assertEquals(consequence1.consequenceOffset, entry1.logicalOffset);
			Assert.assertEquals(consequence1FileOffset, entry1.fileOffset);
			
			IndexEntry entry2 = IndexEntry.read(buffer);
			Assert.assertEquals(consequence2.consequenceOffset, entry2.logicalOffset);
			Assert.assertEquals(consequence2FileOffset, entry2.fileOffset);
		}
	}

	/**
	 * Tests that the management of AVM artifacts works correctly, even over topic delete and recreate.
	 */
	@Test
	public void testProgrammableTopicArtifacts() throws Throwable {
		File directory = _folder.newFolder();
		TopicName topic = TopicName.fromString("fake");
		byte[] code1 = new byte[] {1};
		byte[] code2 = new byte[] {2};
		byte[] arguments = new byte[0];
		byte[] graph1 = new byte[] {1,1};
		byte[] graph2 = new byte[] {1,2};
		byte[] graph3 = new byte[] {1,3};
		File topicDirectory = new File(new File(directory, DiskManager.CONSEQUENCE_TOPICS_DIRECTORY_NAME), topic.string);
		
		// Create the intentions.
		Intention create1 = Intention.createTopic(1L, 1L, topic, UUID.randomUUID(), 1L, code1, arguments);
		Intention put = Intention.put(1L, 2L, topic, UUID.randomUUID(), 1L, new byte[0], new byte[] {1});
		Intention destroy = Intention.destroyTopic(1L, 3L, topic, UUID.randomUUID(), 1L);
		Intention create2 = Intention.createTopic(1L, 4L, topic, UUID.randomUUID(), 1L, code2, arguments);
		
		Consequence create1Consequence = Consequence.createTopic(1L, 1L, 1L, UUID.randomUUID(), 1L, code1, arguments);
		Consequence destroyConsequence = Consequence.destroyTopic(1L, 3L, 2L, UUID.randomUUID(), 1L);
		Consequence create2Consequence = Consequence.createTopic(1L, 4L, 3L, UUID.randomUUID(), 1L, code2, arguments);
		
		LatchedCallbacks callbacks = new LatchedCallbacks();
		DiskManager manager = new DiskManager(directory, callbacks);
		manager.startAndWaitForReady();
		
		// Run the initial CREATE and then make sure the code and graph were written.
		manager.commit(create1, CommitInfo.Effect.VALID, Collections.singletonList(create1Consequence), code1, graph1);
		while (callbacks.commitMutationCount < 1) { callbacks.runOneCommand(); }
		Assert.assertTrue(new File(topicDirectory, LogFileDomain.CODE_NAME_PREFIX + create1.intentionOffset).exists());
		Assert.assertTrue(new File(topicDirectory, LogFileDomain.GRAPH_NAME_PREFIX + create1.intentionOffset).exists());
		
		// Run the PUT and see that the graph changed.
		manager.commit(put, CommitInfo.Effect.VALID, Collections.emptyList(), null, graph2);
		while (callbacks.commitMutationCount < 2) { callbacks.runOneCommand(); }
		Assert.assertTrue(new File(topicDirectory, LogFileDomain.CODE_NAME_PREFIX + create1.intentionOffset).exists());
		Assert.assertFalse(new File(topicDirectory, LogFileDomain.GRAPH_NAME_PREFIX + create1.intentionOffset).exists());
		Assert.assertTrue(new File(topicDirectory, LogFileDomain.GRAPH_NAME_PREFIX + put.intentionOffset).exists());
		
		// Run the DESTROY and observe that nothing changed (since we passed in nulls).
		manager.commit(destroy, CommitInfo.Effect.VALID, Collections.singletonList(destroyConsequence), null, null);
		while (callbacks.commitMutationCount < 3) { callbacks.runOneCommand(); }
		Assert.assertTrue(new File(topicDirectory, LogFileDomain.CODE_NAME_PREFIX + create1.intentionOffset).exists());
		Assert.assertTrue(new File(topicDirectory, LogFileDomain.GRAPH_NAME_PREFIX + put.intentionOffset).exists());
		
		// Run the second CREATE and verify the new code and graph are written.
		manager.commit(create2, CommitInfo.Effect.VALID, Collections.singletonList(create2Consequence), code2, graph3);
		while (callbacks.commitMutationCount < 4) { callbacks.runOneCommand(); }
		manager.stopAndWaitForTermination();
		Assert.assertFalse(new File(topicDirectory, LogFileDomain.CODE_NAME_PREFIX + create1.intentionOffset).exists());
		Assert.assertFalse(new File(topicDirectory, LogFileDomain.GRAPH_NAME_PREFIX + put.intentionOffset).exists());
		Assert.assertTrue(new File(topicDirectory, LogFileDomain.CODE_NAME_PREFIX + create2.intentionOffset).exists());
		Assert.assertTrue(new File(topicDirectory, LogFileDomain.GRAPH_NAME_PREFIX + create2.intentionOffset).exists());
	}


	/**
	 * Used for simple cases where the external test only wants to verify that a call was made when expected.
	 */
	private static class LatchedCallbacks implements IDiskManagerBackgroundCallbacks {
		public Intention expectedMutation;
		public Consequence expectedEvent;
		public int commitMutationCount;
		public int fetchMutationCount;
		public int commitEventCount;
		public int fetchEventCount;
		private Consumer<StateSnapshot> _nextCommand;
		
		public synchronized void runOneCommand() {
			Consumer<StateSnapshot> command = _nextCommand;
			synchronized(this) {
				while (null == _nextCommand) {
					try {
						this.wait();
					} catch (InterruptedException e) {
						Assert.fail("Not used in test");
					}
				}
				command = _nextCommand;
				_nextCommand = null;
				this.notifyAll();
			}
			// We don't use the snapshot in these tests so just pass null.
			command.accept(null);
		}
		
		@Override
		public synchronized void ioEnqueueDiskCommandForMainThread(Consumer<StateSnapshot> command) {
			while (null != _nextCommand) {
				try {
					this.wait();
				} catch (InterruptedException e) {
					Assert.fail("Not used in test");
				}
			}
			_nextCommand = command;
			this.notifyAll();
		}
		
		@Override
		public void mainIntentionWasCommitted(CommittedIntention completed) {
			this.commitMutationCount += 1;
		}
		
		@Override
		public void mainConsequenceWasCommitted(TopicName topic, Consequence completed) {
			this.commitEventCount += 1;
		}
		
		@Override
		public void mainIntentionWasFetched(StateSnapshot snapshot, long previousMutationTermNumber, CommittedIntention record) {
			// We currently just support a single match.
			Assert.assertTrue(record.record.equals(this.expectedMutation));
			this.fetchMutationCount += 1;
		}
		
		@Override
		public void mainConsequenceWasFetched(TopicName topic, Consequence record) {
			// We currently just support a single match.
			Assert.assertTrue(record.equals(this.expectedEvent));
			this.fetchEventCount += 1;
		}
	}
}
