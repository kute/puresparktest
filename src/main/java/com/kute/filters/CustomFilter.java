package com.kute.filters;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.slf4j.Logger;

import javax.servlet.*;
import java.io.IOException;

/**
 * Created by longbai on 2017/6/19.
 */
public class CustomFilter implements Filter {

    private Log logger = LogFactory.getLog(CustomFilter.class);

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {

    }

    @Override
    public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain) throws IOException, ServletException {
        logger.warn("======inside filter==========");
        filterChain.doFilter(servletRequest, servletResponse);
    }

    @Override
    public void destroy() {

    }
}
