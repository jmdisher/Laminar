package com.jeffdisher.laminar.performance;

import com.jeffdisher.laminar.client.ClientConnection;


public interface IPerformanceUnit {

	void startWithClient(ClientConnection client) throws Throwable;

	void exceptionInRun(Throwable t);

	void runIteration(ClientConnection client, int iteration) throws Throwable;

	void finishTestUnderTime(ClientConnection client) throws Throwable;

}
