package com.jeffdisher.laminar.disk;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;

import com.jeffdisher.laminar.avm.TopicContext;
import com.jeffdisher.laminar.logging.Logger;
import com.jeffdisher.laminar.types.ClusterConfig;
import com.jeffdisher.laminar.types.Consequence;
import com.jeffdisher.laminar.types.Intention;
import com.jeffdisher.laminar.types.TopicName;
import com.jeffdisher.laminar.utils.Assert;


/**
 * Used during start-up to read the filesystem, fix any inconsistencies caused by a runtime failure, and return all the
 * information required to restore the system to the state it was in prior to shutdown.
 * This is kept mostly distinct from DiskManager and LogFileDomain but they can later be coalesced if this causes too
 * much duplication of persistent shape knowledge or assumptions.
 */
public class RecoveredState {
	/**
	 * Reads the state from the given dataDirectory, making repairs where required.
	 * General outline of actions:
	 * 1) Read the intention index, since that is the true atomic progress measure through the stream.
	 * 2) Verify that the intention log is not longer than this, truncate it if so.
	 * 3) Walk all topics, doing the same thing:
	 *    -verify the consequence index
	 *    -truncate it if longer than the index implies it should be
	 * 
	 * We also have a few other constraints to apply to topics:
	 * -truncate the index and then the log if longer than the last intention (always index first since it is written last)
	 * -delete any code or graphs with a later intention offset
	 * -delete any shadowed code or graph files remaining
	 * -check the last consequence to see if it is a DESTROY_TOPIC:
	 *  -if so, load only the last consequence offset into memory (in case it is recreated later)
	 *  -if not, load the code and graph into memory as valid topics
	 * 
	 * @param dataDirectory The root of the data directory used by a Laminar instance (must be a directory).
	 * @param defaultConfig The config to use if there wasn't one on disk (since the default is never saved).
	 * @return The state which can be used to recover the system or null if there was no state on-disk.
	 * @throws IOException Something went wrong while operating on the directory.
	 */
	public static RecoveredState readStateFromRootDirectory(Logger logger, File dataDirectory, ClusterConfig defaultConfig) throws IOException {
		// We only proceed if the directory exists and is empty.
		RecoveredState state = null;
		if (dataDirectory.isDirectory() && (0 != dataDirectory.listFiles().length)) {
			state = _readStateFromExistingStructure(logger, dataDirectory, defaultConfig);
		}
		return state;
	}


	private static RecoveredState _readStateFromExistingStructure(Logger logger, File dataDirectory, ClusterConfig defaultConfig) throws IOException {
		File intentionDirectory = new File(dataDirectory, DiskManager.INTENTION_DIRECTORY_NAME);
		File topLevelConsequenceDirectory = new File(dataDirectory, DiskManager.CONSEQUENCE_TOPICS_DIRECTORY_NAME);
		
		// The intention index is the last thing written so it is the first thing we read.
		// We need to add an extra byte to the record size interpretation for intentions for CommitInfo.Effect.
		ByteBuffer intentBuffer = _readLastExtentFromLogDirectory(logger, intentionDirectory, false, Byte.BYTES);
		// Handle the case where the log is empty.
		RecoveredState state;
		if (null == intentBuffer) {
			state = new RecoveredState(defaultConfig, 0L, 0L, new HashMap<>(), new HashMap<>());
		} else {
			state = _readNonEmptyLog(logger, defaultConfig, intentionDirectory, topLevelConsequenceDirectory, intentBuffer);
		}
		return state;
	}

	private static RecoveredState _readNonEmptyLog(Logger logger, ClusterConfig defaultConfig, File intentionDirectory, File topLevelConsequenceDirectory, ByteBuffer intentBuffer) throws IOException {
		// We want the size of the serialized intention.
		Short.toUnsignedInt(intentBuffer.getShort());
		// Next, we need the effect.
		intentBuffer.get();
		// Finally, we deserialize the extent.
		Intention intention = Intention.deserializeFrom(intentBuffer);
		long maximumIntentionOffset = intention.intentionOffset;
		File configFile = _getLatestFile(intentionDirectory, LogFileDomain.CONFIG_NAME_PREFIX, maximumIntentionOffset);
		ClusterConfig configToReturn;
		if (null != configFile) {
			byte[] buffer = Files.readAllBytes(configFile.toPath());
			configToReturn = ClusterConfig.deserialize(buffer);
		} else {
			configToReturn = defaultConfig;
		}
		
		// Find all the topics and do something similar with them.
		File[] topicDirectories = topLevelConsequenceDirectory.listFiles((file) -> file.isDirectory());
		Map<TopicName, TopicContext> activeTopics = new HashMap<>();
		Map<TopicName, Long> nextConsequenceOffsetByTopic = new HashMap<>();
		for (File directory : topicDirectories) {
			TopicName topic = TopicName.fromString(directory.getName());
			// Get the last consequence to see if it is a DESTROY_TOPIC.
			// Note that we may need to walk backward through the topic to find the last entry with a valid intention offset.
			Consequence consequence = null;
			boolean isSuccessfulRead = false;
			boolean shouldTruncate = false;
			while (!isSuccessfulRead) {
				ByteBuffer consequenceBuffer = _readLastExtentFromLogDirectory(logger, directory, shouldTruncate, 0);
				if (null != consequenceBuffer) {
					// We want the size of the serialized consequence.
					Short.toUnsignedInt(consequenceBuffer.getShort());
					// Finally, we deserialize the extent.
					Consequence check = Consequence.deserializeFrom(consequenceBuffer);
					if (check.intentionOffset <= maximumIntentionOffset) {
						consequence = check;
						isSuccessfulRead = true;
					} else {
						// This consequence is too late so remove it and try again.
						shouldTruncate = true;
					}
				} else {
					// The topic is empty.
					isSuccessfulRead = true;
				}
			}
			// We will only add this to the maps if we managed to read something.
			if (null != consequence) {
				if (Consequence.Type.TOPIC_DESTROY != consequence.type) {
					// This is an active topic.
					TopicContext context = new TopicContext();
					activeTopics.put(topic, context);
					
					// Check the code (even present as an empty file if not programmable).
					File codeFile = _getLatestFile(directory, LogFileDomain.CODE_NAME_PREFIX, maximumIntentionOffset);
					context.transformedCode = Files.readAllBytes(codeFile.toPath());
					// Check the graph (even present as an empty file if not programmable).
					File graphFile = _getLatestFile(directory, LogFileDomain.GRAPH_NAME_PREFIX, maximumIntentionOffset);
					context.objectGraph = Files.readAllBytes(graphFile.toPath());
				}
				nextConsequenceOffsetByTopic.put(topic, consequence.consequenceOffset + 1L);
			}
		}
		return new RecoveredState(configToReturn, intention.termNumber, intention.intentionOffset, activeTopics, nextConsequenceOffsetByTopic);
	}

	private static ByteBuffer _readLastExtentFromLogDirectory(Logger logger, File directory, boolean shouldRemoveLastEntry, int addToRecordSize) throws IOException {
		// The index represents the atomic write so we use that to find the last valid extent.
		File indexFile = new File(directory, LogFileDomain.INDEX_FILE_NAME);
		long fileOffsetToRead = -1L;
		try (FileInputStream stream = new FileInputStream(indexFile)) {
			FileChannel intentIndex = stream.getChannel();
			long offsetOfLastEntry = indexFile.length() - IndexEntry.BYTES;
			if (shouldRemoveLastEntry) {
				// The index contains data beyond the last valid index entry so truncate it (requires a writable stream).
				try (FileOutputStream recovery = new FileOutputStream(indexFile, true)) {
					long validLength = indexFile.length() - IndexEntry.BYTES;
					logger.important("RECOVERY:  Truncated topic index " + indexFile + " from " + indexFile.length() + " to " + validLength);
					recovery.getChannel().truncate(validLength);
				}
				// Correct the offset to reflect the change.
				offsetOfLastEntry -= IndexEntry.BYTES;
			}
			// Make sure we haven't left just an empty file.
			if (indexFile.length() > 0) {
				ByteBuffer indexEntry = ByteBuffer.allocate(IndexEntry.BYTES);
				int readSize = intentIndex.read(indexEntry, offsetOfLastEntry);
				Assert.assertTrue(indexEntry.capacity() == readSize);
				indexEntry.flip();
				IndexEntry finalEntry = IndexEntry.read(indexEntry);
				fileOffsetToRead = finalEntry.fileOffset;
			}
		}
		
		// Make sure that we found an offset.
		ByteBuffer bufferToReturn = null;
		if (fileOffsetToRead >= 0L) {
			// We can use this to read the final entry from the log file.
			File logFile = new File(directory, LogFileDomain.LOG_FILE_NAME);
			try (FileInputStream stream = new FileInputStream(logFile)) {
				FileChannel intentLog = stream.getChannel();
				bufferToReturn = ByteBuffer.allocate((int)(logFile.length() - fileOffsetToRead));
				int readSize = intentLog.read(bufferToReturn, fileOffsetToRead);
				Assert.assertTrue(bufferToReturn.capacity() == readSize);
				bufferToReturn.flip();
				
				// Before we return this, we need to check if the file had corruption beyond what the index describes so read its size (adding constant, if required).
				bufferToReturn.mark();
				int recordSize = Short.toUnsignedInt(bufferToReturn.getShort()) + addToRecordSize;
				if (bufferToReturn.remaining() > recordSize) {
					// This file needs to be truncated.
					try (FileOutputStream recovery = new FileOutputStream(logFile, true)) {
						long validLength = logFile.length() - (bufferToReturn.remaining() - recordSize);
						logger.important("RECOVERY:  Truncated log file " + logFile + " from " + logFile.length() + " to " + validLength);
						recovery.getChannel().truncate(validLength);
					}
				}
				bufferToReturn.reset();
			}
		}
		return bufferToReturn;
	}

	private static File _getLatestFile(File directory, String prefix, long maximumIntentionOffset) {
		File[] matches = directory.listFiles((file, name) -> name.startsWith(prefix));
		// Find the matching file.
		long greatestSuffix = -1;
		File matchingFile = null;
		for (File check : matches) {
			long suffix = Long.parseLong(check.getName().split("\\.")[1]);
			if ((suffix > greatestSuffix) && (suffix <= maximumIntentionOffset)) {
				greatestSuffix = suffix;
				matchingFile = check;
			}
		}
		// Clean any other stale or future files.
		for (File check : matches) {
			if (check != matchingFile) {
				boolean didDelete = check.delete();
				if (!didDelete) {
					throw new IllegalStateException("Failed to delete stale/invalid file: " + check);
				}
			}
		}
		return matchingFile;
	}


	// Information required for NodeState and misc components.
	public final ClusterConfig config;
	public final long currentTermNumber;
	public final long lastCommittedIntentionOffset;
	// Information required for IntentionExecutor.
	public final Map<TopicName, TopicContext> activeTopics;
	public final Map<TopicName, Long> nextConsequenceOffsetByTopic;

	private RecoveredState(ClusterConfig config, long currentTermNumber, long lastCommittedIntentionOffset, Map<TopicName, TopicContext> activeTopics, Map<TopicName, Long> nextConsequenceOffsetByTopic) {
		this.config = config;
		this.currentTermNumber = currentTermNumber;
		this.lastCommittedIntentionOffset = lastCommittedIntentionOffset;
		this.activeTopics = activeTopics;
		this.nextConsequenceOffsetByTopic = nextConsequenceOffsetByTopic;
	}
}
