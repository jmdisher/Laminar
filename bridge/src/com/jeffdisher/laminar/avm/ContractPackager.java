package com.jeffdisher.laminar.avm;

import org.aion.avm.utilities.JarBuilder;


/**
 * This just gates access to the packaging helper inside AVM since we don't want anything outside of the bridge
 * component directly knowing about AVM.
 */
public class ContractPackager {
	public static byte[] createJarForClass(Class<?> clazz) {
		return JarBuilder.buildJarForExplicitMainAndClasses(clazz.getName(), clazz);
	}
}
