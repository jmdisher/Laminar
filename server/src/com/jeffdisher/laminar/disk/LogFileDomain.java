package com.jeffdisher.laminar.disk;

import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;

import com.jeffdisher.laminar.utils.Assert;


/**
 * Manages the domain of a log file, which means either the intention log or a topic log.
 * The DiskManager deals with high-level management of IO requests/scheduling and life-cycle of these domains while the
 * domain manages the actual interaction with the specific files.
 */
public class LogFileDomain implements Closeable {
	public static final String LOG_FILE_NAME = "logs.0";
	public static final String INDEX_FILE_NAME = "index.0";

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
		return new LogFileDomain(logOutputStream, logInputChannel, indexOutputStream, indexInputStream, (int)logFileSize);
	}


	private final FileOutputStream _writeLogFile;
	private final FileChannel _readLogFile;
	private final FileOutputStream _writeIndexFile;
	private final FileChannel _readIndexFile;
	private int _logFileSize;

	private LogFileDomain(FileOutputStream logFile, FileChannel readLogFile, FileOutputStream indexFile, FileChannel readIndexFile, int logFileSize) {
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

	@Override
	public void close() throws IOException {
		_writeLogFile.close();
		_readLogFile.close();
		_writeIndexFile.close();
		_readIndexFile.close();
	}
}
