package com.jeffdisher.laminar.types;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.UUID;

import com.jeffdisher.laminar.utils.Assert;


/**
 * A single entry in the ClusterConfig, representing a single node.
 * Note that the cluster-facing and client-facing sockets are defined independently.
 * While these instances normally appear as part of a whole ClusterConfig, there are situations where it makes sense to
 * reason about them or serialize/deserialize them, alone (where the node describes itself, for example).
 */
public final class ConfigEntry {
	public static final int IPV4_BYTE_SIZE = 4;
	public static final int IPV6_BYTE_SIZE = 16;
	public static final int MAX_PORT = (64 * 1024) - 1;

	public static ConfigEntry deserializeFrom(ByteBuffer buffer) {
		UUID nodeUuid = new UUID(buffer.getLong(), buffer.getLong());
		InetSocketAddress cluster = _readPair(buffer);
		InetSocketAddress client = _readPair(buffer);
		return new ConfigEntry(nodeUuid, cluster, client);
	}


	private static IllegalArgumentException _parseError() {
		throw new IllegalArgumentException("ClusterConfig invalid");
	}

	private static InetSocketAddress _readPair(ByteBuffer buffer) {
		byte ipLength = buffer.get();
		if ((IPV4_BYTE_SIZE != ipLength) && (IPV6_BYTE_SIZE != ipLength)) {
			throw _parseError();
		}
		byte[] ip = new byte[ipLength];
		buffer.get(ip);
		int port = Short.toUnsignedInt(buffer.getShort());
		try {
			return new InetSocketAddress(InetAddress.getByAddress(ip), port);
		} catch (UnknownHostException e) {
			// Only happens if the IP is the incorrect length and we already checked this.
			throw Assert.unexpected(e);
		}
	}


	public final UUID nodeUuid;
	public final InetSocketAddress cluster;
	public final InetSocketAddress client;
	
	public ConfigEntry(UUID nodeUuid, InetSocketAddress cluster, InetSocketAddress client) {
		this.nodeUuid = nodeUuid;
		this.cluster = cluster;
		this.client = client;
	}

	@Override
	public boolean equals(Object arg0) {
		boolean isEqual = (this == arg0);
		if (!isEqual && (null != arg0) && (this.getClass() == arg0.getClass())) {
			ConfigEntry object = (ConfigEntry) arg0;
			isEqual = this.nodeUuid.equals(object.nodeUuid)
					&& this.cluster.equals(object.cluster)
					&& this.client.equals(object.client)
			;
		}
		return isEqual;
	}

	@Override
	public int hashCode() {
		return this.nodeUuid.hashCode()
				^ this.cluster.hashCode()
				^ this.client.hashCode()
		;
	}

	@Override
	public String toString() {
		return "ConfigEntry(UUID: " + this.nodeUuid + ", Cluster: " + this.cluster.toString() + ", Client: " + this.client + ")";
	}

	public int serializedSize() {
		int bufferSize = 0;
		bufferSize += (2 * Long.BYTES);
		// The port is always a u16 but the IP can be 4 or 16 bytes, and each one has a byte to describe which.
		bufferSize += Byte.BYTES + this.cluster.getAddress().getAddress().length + Short.BYTES;
		bufferSize += Byte.BYTES + this.client.getAddress().getAddress().length + Short.BYTES;
		return bufferSize;
	}

	public void serializeInto(ByteBuffer buffer) {
		buffer.putLong(this.nodeUuid.getMostSignificantBits());
		buffer.putLong(this.nodeUuid.getLeastSignificantBits());
		_writePair(buffer, this.cluster);
		_writePair(buffer, this.client);
	}


	private void _writePair(ByteBuffer buffer, InetSocketAddress pair) {
		byte[] ip = pair.getAddress().getAddress();
		// These are the sizes defined within the InetAddress documentation
		Assert.assertTrue((IPV4_BYTE_SIZE == ip.length) || (IPV6_BYTE_SIZE == ip.length));
		short port = (short)pair.getPort();
		byte ipLength = (byte)ip.length;
		buffer.put(ipLength);
		buffer.put(ip);
		buffer.putShort(port);
	}
}
