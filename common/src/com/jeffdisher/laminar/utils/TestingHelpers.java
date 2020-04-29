package com.jeffdisher.laminar.utils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;


/**
 * A miscellaneous collection of helper methods used in many different tests.
 */
public class TestingHelpers {
	/**
	 * Creates a server socket bound to the localhost.
	 * 
	 * @param port The port to bind.
	 * @return A bound ServerSocketChannel.
	 * @throws IOException Something went wrong.
	 */
	public static ServerSocketChannel createServerSocket(int port) throws IOException {
		ServerSocketChannel socket = ServerSocketChannel.open();
		InetSocketAddress clientAddress = new InetSocketAddress(port);
		socket.bind(clientAddress);
		return socket;
	}

	/**
	 * Reads a framed message from the source, returning the contents of the frame.
	 * 
	 * @param source The stream from which to read the message.
	 * @return The raw contents of the message frame.
	 * @throws IOException Something went wrong.
	 */
	public static byte[] readMessageInFrame(InputStream source) throws IOException {
		byte[] frameSize = new byte[Short.BYTES];
		int read = 0;
		while (read < frameSize.length) {
			read += source.read(frameSize, read, frameSize.length - read);
		}
		int sizeToRead = Short.toUnsignedInt(ByteBuffer.wrap(frameSize).getShort());
		byte[] frame = new byte[sizeToRead];
		read = 0;
		while (read < frame.length) {
			read += source.read(frame, read, frame.length - read);
		}
		return frame;
	}

	/**
	 * Writes raw data into a framed message and writes it to the given sink.
	 * 
	 * @param sink The stream to which the message should be written.
	 * @param raw The raw data to package into a frame and write.
	 * @throws IOException Something went wrong.
	 */
	public static void writeMessageInFrame(OutputStream sink, byte[] raw) throws IOException {
		byte[] frame = new byte[Short.BYTES + raw.length];
		ByteBuffer.wrap(frame)
			.putShort((short)raw.length)
			.put(raw)
			;
		sink.write(frame);
	}

}
