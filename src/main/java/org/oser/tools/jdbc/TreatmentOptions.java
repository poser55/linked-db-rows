package org.oser.tools.jdbc;

import java.util.EnumSet;
import java.util.Set;

/**
 * Options for db init // todo: adapt for db2graph
 */
public enum TreatmentOptions {

	/** false means: do not make updates (only inserts). Default: true */
	OverwriteData (true),

	/** true means: do not write to db. Default: false */
	DryRun(false),

	/** If the data exist, generate insertion scripts anyway */
	ForceInsert(false),

	/** try remapping the primary keys  */
	RemapPrimaryKeys (false);

	private boolean defaultValue;

	TreatmentOptions(boolean defaultValue) {
		this.defaultValue = defaultValue;
	}

	public boolean getDefaultValue() {
		return defaultValue;
	}

	public static boolean getValue(TreatmentOptions key, Set<TreatmentOptions> options) {
		return (options.contains(key)) ? !key.getDefaultValue() : key.getDefaultValue();
	}
}
