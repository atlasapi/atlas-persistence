package org.atlasapi.persistence.logging;

public class NullAdapterLog implements AdapterLog {

	@Override
	public void record(AdapterLogEntry entry) {
	    System.out.println(entry.description() + " : " + entry.exceptionSummary());
		// do nothing
	}

}
