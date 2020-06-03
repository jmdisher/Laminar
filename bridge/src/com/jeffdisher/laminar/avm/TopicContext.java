package com.jeffdisher.laminar.avm;


/**
 * Contains the contextual data associated with a programmable topic.
 * This is mutable since the instance is long-lived but its elements are changed when passed to the AvmBridge.
 */
public class TopicContext {
	public byte[] transformedCode;
	public byte[] objectGraph;
}
