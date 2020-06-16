package com.jeffdisher.laminar.disk;

import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;

import com.jeffdisher.laminar.types.ClusterConfig;
import com.jeffdisher.laminar.utils.Assert;


/**
 * Manages the domain of a log file, which means either the intention log or a topic log.
 * The DiskManager deals with high-level management of IO requests/scheduling and life-cycle of these domains while the
 * domain manages the actual interaction with the specific files.
 */
public class LogFileDomain implements Closeable {
	public static final String LOG_FILE_NAME = "logs.0";
	public static final String INDEX_FILE_NAME = "index.0";
	public static final String CODE_NAME_PREFIX = "code.";
	public static final String GRAPH_NAME_PREFIX = "graph.";
	public static final String CONFIG_NAME_PREFIX = "config.";

	public static LogFileDomain openFromDirectory(File directory) throws IOException {
		File logFile = new File(directory, LOG_FILE_NAME);
		FileOutputStream logOutputStream = new FileOutputStream(logFile, true);
		FileChannel logInputChannel = FileChannel.open(logFile.toPath(), StandardOpenOption.READ);
		
		File indexFile = new File(directory, INDEX_FILE_NAME);
		FileOutputStream indexOutputStream = null;
		FileChannel indexInputStream;
		try {
			indexOutputStream = new FileOutputStream(indexFile, true);
			indexInputStream = FileChannel.open(indexFile.toPath(), StandardOpenOption.READ);
		} catch (FileNotFoundException e) {
			logOutputStream.close();
			logInputChannel.close();
			if (null != indexOutputStream) {
				indexOutputStream.close();
			}
			throw e;
		}
		long logFileSize = logFile.length();
		// We want to keep these log files small - in the future, we will split them, but we just add this assertion, now.
		Assert.assertTrue(logFileSize < Integer.MAX_VALUE);
		return new LogFileDomain(directory, logOutputStream, logInputChannel, indexOutputStream, indexInputStream, (int)logFileSize);
	}


	private final File _directory;
	private final FileOutputStream _writeLogFile;
	private final FileChannel _readLogFile;
	private final FileOutputStream _writeIndexFile;
	private final FileChannel _readIndexFile;
	private int _logFileSize;

	private LogFileDomain(File directory, FileOutputStream logFile, FileChannel readLogFile, FileOutputStream indexFile, FileChannel readIndexFile, int logFileSize) {
		_directory = directory;
		_writeLogFile = logFile;
		_readLogFile = readLogFile;
		_writeIndexFile = indexFile;
		_readIndexFile = readIndexFile;
		_logFileSize = logFileSize;
	}

	public void syncLog() throws IOException {
		_writeLogFile.getChannel().force(true);
	}

	public void syncIndex() throws IOException {
		_writeIndexFile.getChannel().force(true);
	}

	public int getLogFileSizeBytes() {
		return _logFileSize;
	}

	public void appendToLogFile(ByteBuffer buffer) throws IOException {
		_writeLogFile.write(buffer.array());
		_logFileSize += buffer.capacity();
	}

	public void appendToIndexFile(IndexEntry entry) throws IOException {
		ByteBuffer entryBuffer = ByteBuffer.allocate(IndexEntry.BYTES);
		entry.writeToBuffer(entryBuffer);
		_writeIndexFile.write(entryBuffer.array());
	}

	public ByteBuffer readExtentAtOffset(long logicalOffset) throws IOException {
		// Read the index to find this offset in the file, and the next one (or end of file) so we can read the extent.
		// (since we don't currently support compaction, we can find where this is in the index with a static calculation)
		long offsetOfIndexEntry = (logicalOffset - 1L) * (long)IndexEntry.BYTES;
		ByteBuffer buffer = ByteBuffer.allocate(IndexEntry.BYTES * 2);
		int readSize = _readIndexFile.read(buffer, offsetOfIndexEntry);
		buffer.flip();
		IndexEntry entry = IndexEntry.read(buffer);
		Assert.assertTrue(logicalOffset == entry.logicalOffset);
		int endOfRead;
		if (IndexEntry.BYTES == readSize) {
			// This is the end of the file (log files are small so we can cast to int).
			endOfRead = (int)_readLogFile.size();
		} else {
			Assert.assertTrue((2 * IndexEntry.BYTES) == readSize);
			// We are in the middle of the file.
			endOfRead = IndexEntry.read(buffer).fileOffset;
		}
		ByteBuffer total = ByteBuffer.allocate(endOfRead - entry.fileOffset);
		readSize = _readLogFile.read(total, entry.fileOffset);
		total.flip();
		Assert.assertTrue(readSize == (endOfRead - entry.fileOffset));
		return total;
	}

	/**
	 * Writes the given transformedCode as the code for the given finalIntentionOffset, and forces it to sync to disk.
	 * 
	 * @param finalIntentionOffset The offset of the final intention in the current set of changes being written.
	 * @param transformedCode The transformed code for a new topic (empty if non-programmable).
	 * @throws IOException Something went wrong when opening, writing, or synchronizing the file.
	 */
	public void writeCode(long finalIntentionOffset, byte[] transformedCode) throws IOException {
		File codeFile = new File(_directory, CODE_NAME_PREFIX + finalIntentionOffset);
		try (FileOutputStream codeOutputStream = new FileOutputStream(codeFile, true)) {
			codeOutputStream.write(transformedCode);
			codeOutputStream.getChannel().force(true);
		}
	}

	/**
	 * Writes the given objectGraph as the object graph for the given finalIntentionOffset, and forces it to sync to disk.
	 * 
	 * @param finalIntentionOffset The offset of the final intention in the current set of changes being written.
	 * @param objectGraph The object graph for this topic at the end of this set of changes.
	 * @throws IOException Something went wrong when opening, writing, or synchronizing the file.
	 */
	public void writeObjectGraph(long finalIntentionOffset, byte[] objectGraph) throws IOException {
		File graphFile = new File(_directory, GRAPH_NAME_PREFIX + finalIntentionOffset);
		try (FileOutputStream graphOutputStream = new FileOutputStream(graphFile, true)) {
			graphOutputStream.write(objectGraph);
			graphOutputStream.getChannel().force(true);
		}
	}

	/**
	 * Writes the given newConfig as the config for the given finalIntentionOffset, and forces it to sync to disk.
	 * 
	 * @param finalIntentionOffset The offset of the final intention in the current set of changes being written.
	 * @param newConfig An updated ClusterConfig.
	 * @throws IOException Something went wrong when opening, writing, or synchronizing the file.
	 */
	public void writeClusterConfig(long finalIntentionOffset, ClusterConfig newConfig) throws IOException {
		File graphFile = new File(_directory, CONFIG_NAME_PREFIX + finalIntentionOffset);
		try (FileOutputStream graphOutputStream = new FileOutputStream(graphFile, true)) {
			graphOutputStream.write(newConfig.serialize());
			graphOutputStream.getChannel().force(true);
		}
	}

	/**
	 * Deletes any transformed code written to the topic for any write attempt other than the one for finalIntentionOffset.
	 * 
	 * @param finalIntentionOffset The offset of the final intention in the current set of changes being written.
	 */
	public void purgeStaleCode(long finalIntentionOffset) {
		String latestFile = CODE_NAME_PREFIX + finalIntentionOffset;
		File[] stale = _directory.listFiles((file, name) -> name.startsWith(CODE_NAME_PREFIX) && !name.equals(latestFile));
		if (0 != stale.length) {
			Assert.assertTrue(1 == stale.length);
			boolean didDelete = stale[0].delete();
			Assert.assertTrue(didDelete);
		}
	}

	/**
	 * Deletes any object graph written to the topic for any write attempt other than the one for finalIntentionOffset.
	 * 
	 * @param finalIntentionOffset The offset of the final intention in the current set of changes being written.
	 */
	public void purgeStaleObjectGraphs(long finalIntentionOffset) {
		String latestFile = GRAPH_NAME_PREFIX + finalIntentionOffset;
		File[] stale = _directory.listFiles((file, name) -> name.startsWith(GRAPH_NAME_PREFIX) && !name.equals(latestFile));
		if (0 != stale.length) {
			Assert.assertTrue(1 == stale.length);
			boolean didDelete = stale[0].delete();
			Assert.assertTrue(didDelete);
		}
	}

	/**
	 * Deletes any cluster config associated with any write attempt other than the one for finalIntentionOffset.
	 * 
	 * @param finalIntentionOffset The offset of the final intention in the current set of changes being written.
	 */
	public void purgeStaleConfigs(long finalIntentionOffset) {
		String latestFile = CONFIG_NAME_PREFIX + finalIntentionOffset;
		File[] stale = _directory.listFiles((file, name) -> name.startsWith(CONFIG_NAME_PREFIX) && !name.equals(latestFile));
		if (0 != stale.length) {
			Assert.assertTrue(1 == stale.length);
			boolean didDelete = stale[0].delete();
			Assert.assertTrue(didDelete);
		}
	}

	@Override
	public void close() throws IOException {
		_writeLogFile.close();
		_readLogFile.close();
		_writeIndexFile.close();
		_readIndexFile.close();
	}
}
