package com.jeffdisher.laminar.disk;

import java.nio.ByteBuffer;


/**
 * The underlying data structure written into the index files.
 */
public class IndexEntry {
	public static final int BYTES = Long.BYTES + Integer.BYTES;
	
	public static IndexEntry create(long logicalOffset, int fileOffset) {
		return new IndexEntry(logicalOffset, fileOffset);
	}
	
	public static IndexEntry read(ByteBuffer buffer) {
		long logicalOffset = buffer.getLong();
		int fileOffset = buffer.getInt();
		return new IndexEntry(logicalOffset, fileOffset);
	}
	
	public final long logicalOffset;
	public final int fileOffset;
	
	private IndexEntry(long logicalOffset, int fileOffset) {
		this.logicalOffset = logicalOffset;
		this.fileOffset = fileOffset;
	}
	
	public void writeToBuffer(ByteBuffer buffer) {
		buffer.putLong(this.logicalOffset);
		buffer.putInt(this.fileOffset);
	}
}
