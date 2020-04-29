package com.jeffdisher.laminar.network.p2p;

import java.nio.ByteBuffer;


/**
 * Common interface of the payloads kept in UpstreamResponse instances.
 */
public interface IUpstreamPayload {
	int serializedSize();
	void serializeInto(ByteBuffer buffer);
}
