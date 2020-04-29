package com.jeffdisher.laminar.network.p2p;

import java.nio.ByteBuffer;


/**
 * Common interface of the payloads kept in DownstreamMessage instances.
 */
public interface IDownstreamPayload {
	int serializedSize();
	void serializeInto(ByteBuffer buffer);
}
