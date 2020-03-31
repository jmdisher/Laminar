package com.jeffdisher.laminar.utils;


/**
 * A convenience class for describing states which either represent static usage errors of internal components or
 * error states in the program's state machine.
 * This is not used for parameter validation (except maybe during prototyping), but to point out actual bugs.
 */
public class Assert {
	/**
	 * Fails when a throwable was encountered somewhere it was not considered possible.
	 * This method will throw AssertionError.
	 * 
	 * @param throwable The unexpected exception.
	 * @return Null (just there for the caller to tell the compiler it is throwing something).
	 */
	public static AssertionError unexpected(Throwable throwable) {
		throw new AssertionError("Unexpected throwable", throwable);
	}

	/**
	 * Fails when a statement which must be true but is not.
	 * 
	 * @param flag A statement which MUST be true for the program to be in a valid state.
	 */
	public static void assertTrue(boolean flag) {
		if (!flag) {
			throw new AssertionError("Statement MUST be true");
		}
	}
}
