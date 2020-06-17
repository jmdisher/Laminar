package com.jeffdisher.laminar.disk;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;

import com.jeffdisher.laminar.avm.TopicContext;
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
	public static RecoveredState readStateFromRootDirectory(File dataDirectory, ClusterConfig defaultConfig) throws IOException {
		// We only proceed if the directory exists and is empty.
		RecoveredState state = null;
		if (dataDirectory.isDirectory() && (0 != dataDirectory.listFiles().length)) {
			state = _readStateFromExistingStructure(dataDirectory, defaultConfig);
		}
		return state;
	}


	private static RecoveredState _readStateFromExistingStructure(File dataDirectory, ClusterConfig defaultConfig) throws IOException {
		File intentionDirectory = new File(dataDirectory, DiskManager.INTENTION_DIRECTORY_NAME);
		File topLevelConsequenceDirectory = new File(dataDirectory, DiskManager.CONSEQUENCE_TOPICS_DIRECTORY_NAME);
		
		// The intention index is the last thing written so it is the first thing we read.
		ByteBuffer intentBuffer = _readLastExtentFromLogDirectory(intentionDirectory, -1L);
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
			ByteBuffer consequenceBuffer = _readLastExtentFromLogDirectory(directory, maximumIntentionOffset);
			// We want the size of the serialized consequence.
			Short.toUnsignedInt(consequenceBuffer.getShort());
			// Finally, we deserialize the extent.
			Consequence consequence = Consequence.deserializeFrom(consequenceBuffer);
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
		return new RecoveredState(configToReturn, intention.termNumber, intention.intentionOffset, activeTopics, nextConsequenceOffsetByTopic);
	}

	private static ByteBuffer _readLastExtentFromLogDirectory(File directory, long maximumIntentionOffset) throws IOException {
		// The index represents the atomic write so we use that to find the last valid extent.
		File indexFile = new File(directory, LogFileDomain.INDEX_FILE_NAME);
		long fileOffsetToRead;
		try (FileInputStream stream = new FileInputStream(indexFile)) {
			FileChannel intentIndex = stream.getChannel();
			ByteBuffer indexEntry = ByteBuffer.allocate(IndexEntry.BYTES);
			long offsetOfLastEntry = indexFile.length() - IndexEntry.BYTES;
			long offsetOfMaxEntry = (-1L == maximumIntentionOffset)
					? offsetOfLastEntry
					: (maximumIntentionOffset - 1L) * (long)IndexEntry.BYTES;
			if (offsetOfMaxEntry < offsetOfLastEntry) {
				throw Assert.unimplemented("Truncate file");
			}
			intentIndex.read(indexEntry, offsetOfLastEntry);
			indexEntry.flip();
			IndexEntry finalEntry = IndexEntry.read(indexEntry);
			fileOffsetToRead = finalEntry.fileOffset;
		}
		
		// We can use this to read the final entry from the log file.
		File intentLogFile = new File(directory, LogFileDomain.LOG_FILE_NAME);
		ByteBuffer bufferToReturn;
		try (FileInputStream stream = new FileInputStream(intentLogFile)) {
			FileChannel intentLog = stream.getChannel();
			bufferToReturn = ByteBuffer.allocate((int)(intentLogFile.length() - fileOffsetToRead));
			intentLog.read(bufferToReturn, fileOffsetToRead);
			bufferToReturn.flip();
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
