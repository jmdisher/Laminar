package com.jeffdisher.laminar.network.p2p;

import java.nio.ByteBuffer;

import com.jeffdisher.laminar.utils.Assert;


/**
 * An instance of a message response along the pear-to-peer layer.  These are meant to be sent on the node's "incoming"
 * connections in response to a DownstreamMessage received on the same socket.
 */
public class UpstreamResponse {
	public static UpstreamResponse peerState(long lastReceivedMutationOffset) {
		return new UpstreamResponse(Type.PEER_STATE, UpstreamPayload_PeerState.create(lastReceivedMutationOffset));
	}

	public static UpstreamResponse receivedMutations(long lastReceivedMutationOffset) {
		return new UpstreamResponse(Type.RECEIVED_INTENTIONS, UpstreamPayload_ReceivedIntentions.create(lastReceivedMutationOffset));
	}

	public static UpstreamResponse castVote(long termNumber) {
		return new UpstreamResponse(Type.CAST_VOTE, UpstreamPayload_CastVote.create(termNumber));
	}

	public static UpstreamResponse deserializeFrom(ByteBuffer buffer) {
		byte typeByte = buffer.get();
		if ((typeByte < 0) || (typeByte >= Type.values().length)) {
			throw _parseError();
		}
		Type type = Type.values()[typeByte];
		IUpstreamPayload payload;
		switch (type) {
		case PEER_STATE:
			payload = UpstreamPayload_PeerState.deserializeFrom(buffer);
			break;
		case RECEIVED_INTENTIONS:
			payload = UpstreamPayload_ReceivedIntentions.deserializeFrom(buffer);
			break;
		case CAST_VOTE:
			payload = UpstreamPayload_CastVote.deserializeFrom(buffer);
			break;
		case INVALID:
			throw _parseError();
		default:
			throw Assert.unreachable("Case not handled");
		}
		return new UpstreamResponse(type, payload);
	}


	private static IllegalArgumentException _parseError() {
		throw new IllegalArgumentException("UpstreamResponse invalid");
	}


	public final Type type;
	public final IUpstreamPayload payload;
	
	private UpstreamResponse(Type type, IUpstreamPayload payload) {
		this.type = type;
		this.payload = payload;
	}

	public int serializedSize() {
		return Byte.BYTES + this.payload.serializedSize();
	}

	public void serializeInto(ByteBuffer buffer) {
		buffer.put((byte)this.type.ordinal());
		this.payload.serializeInto(buffer);
	}

	@Override
	public String toString() {
		return "UpstreamResponse(type=" + this.type + ", payload=" + this.payload + ")";
	}


	public static enum Type {
		INVALID,
		PEER_STATE,
		RECEIVED_INTENTIONS,
		CAST_VOTE,
	}
}
