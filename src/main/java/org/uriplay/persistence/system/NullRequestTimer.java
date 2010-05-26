package org.uriplay.persistence.system;

import javax.servlet.http.HttpServletResponse;

/**
 * Null object {@link RequestTimer}.
 * 
 * @author Robert Chatley
 * @author Lee Denison
 */
public class NullRequestTimer implements RequestTimer {

	public void nest() {
		// TODO Auto-generated method stub
	}

	public void outputTo(HttpServletResponse response) {
		// TODO Auto-generated method stub
	}

	public void start(Object target, String uri) {
		// TODO Auto-generated method stub
	}

	public void stop(Object target, String uri) {
		// TODO Auto-generated method stub
	}

	public void unnest() {
		// TODO Auto-generated method stub
	}
	
	@Override
	public int hashCode() {
		return 42;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		} 
		return obj instanceof NullRequestTimer;
	}

}
