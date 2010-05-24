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

package org.uriplay.persistence.servlet;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.web.servlet.ModelAndView;
import org.springframework.web.servlet.handler.SimpleMappingExceptionResolver;

/**
 * {@link StatusCodeAwareException}s can present an HTTP status code to return to the user, instead
 * of mapping a JSP.
 * 
 * @author Robert Chatley (robert@metabroadcast.com)
 */
public class StatusCodeExceptionResolver extends SimpleMappingExceptionResolver {

	@Override
	public ModelAndView resolveException(HttpServletRequest request, HttpServletResponse response, Object handler, Exception ex) {

		if (ex instanceof StatusCodeAwareException) {
			response.setStatus(((StatusCodeAwareException) ex).getStatusCode());
		}
		
		return super.resolveException(request, response, handler, ex);
	}
}
