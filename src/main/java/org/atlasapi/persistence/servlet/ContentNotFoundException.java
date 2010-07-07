/* Copyright 2009 Meta Broadcast Ltd

Licensed under the Apache License, Version 2.0 (the "License"); you
may not use this file except in compliance with the License. You may
obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing
permissions and limitations under the License. */


package org.atlasapi.persistence.servlet;

import javax.servlet.http.HttpServletResponse;

/**
 * Exception associated with a 404 http status code.
 * 
 * @author Robert Chatley (robert@metabroadcast.com)
 */
public class ContentNotFoundException extends RuntimeException implements StatusCodeAwareException {

	private static final long serialVersionUID = 1L;

	public ContentNotFoundException(String msg) {
		super(msg);
	}

	public ContentNotFoundException(Throwable t) {
		super(t);
	}

	public int getStatusCode() {
		return HttpServletResponse.SC_NOT_FOUND;
	}

}
