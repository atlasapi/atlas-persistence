/* Copyright 2009 British Broadcasting Corporation
 
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

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class ServletUtils {

    protected static final Log logger = 
        LogFactory.getLog(ServletUtils.class);

    @SuppressWarnings("unchecked")
	public static List<String> getHeaders(
        HttpServletRequest request, 
        String name) {
        List<String> result = new ArrayList<String>();

        Enumeration<Object> e = request.getHeaders(name);
        while (e.hasMoreElements()) {
            String[] values = ((String) e.nextElement()).split(",");

            for (String value : values) {
                result.add(value.trim());
            }
        }
        
        return result;
    }

    public static String getHttpMessage(Integer code) {
        switch (code) {
            case HttpServletResponse.SC_NOT_FOUND:
                return "Page not found";
            case HttpServletResponse.SC_METHOD_NOT_ALLOWED:
                return "HTTP Method not allowed";
            case HttpServletResponse.SC_INTERNAL_SERVER_ERROR:
                return "Internal server error";
            case HttpServletResponse.SC_FORBIDDEN:
                return "Forbidden";
            default:
                return null;
        }
    }

    public static HttpMethod getMethod(HttpServletRequest request) {
        return Enum.valueOf(HttpMethod.class, request.getMethod());
    }

    public static String formatUri(String str) {
        String converted = str.toLowerCase()
            .replaceAll("\\s+", "-")
            .replaceAll("[^\\p{L}\\p{Digit}-]", "");

        return converted;
    }

    public static String generateUniqueSuffix(
            String source, 
            List<String> existingNames) 
        throws Exception {
        return generateUniqueSuffix(source, existingNames, "-");
    }

    public static String generateUniqueSuffix(
            String source, 
            List<String> existingNames,
            String separator) 
        throws Exception {
        String suffix = "";
        int number = 2;

        if (existingNames.contains(source)) {
            suffix = separator + number;

            while (existingNames.contains(source + suffix)) {
                suffix = separator + ++number;
            }
        }

        return suffix;
    }

    public static List<String> getCanonicalPath(String path) {
        List<String> result = new ArrayList<String>();
        String[] pathElms = path.split("/");

        for (String pathElm : pathElms) {
            if (pathElm != null && pathElm.length() > 0) {
                result.add(pathElm);
            }
        }

        return result;
    }

    public static String trimSeparator(String uri) {
        if (uri == null) {
            return null;
        }

        int idx = uri.length() - 1; 
        while (idx >= 0 && uri.charAt(idx) == '/') {
            idx--;
        }

        return uri.substring(0, idx + 1);
    }

}
