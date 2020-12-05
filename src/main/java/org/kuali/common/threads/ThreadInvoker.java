/**
 * Copyright 2004-2013 The Kuali Foundation
 *
 * Licensed under the Educational Community License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.opensource.org/licenses/ecl2.php
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.kuali.common.threads;

import java.text.NumberFormat;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThreadInvoker {
    private final Logger logger = LoggerFactory.getLogger(ThreadHandlerFactory.class);
    ThreadHandlerFactory factory = new ThreadHandlerFactory();
    NumberFormat nf = getNumberFormat();

    protected NumberFormat getNumberFormat() {
        NumberFormat nf = NumberFormat.getInstance();
        nf.setMaximumFractionDigits(2);
        nf.setMinimumFractionDigits(2);
        nf.setGroupingUsed(false);
        return nf;
    }

    public <T> ExecutionStatistics invokeThreads(ThreadHandlerContext<T> context) {
        return invokeThreads(context, "Executing ");
    }

    public <T> ExecutionStatistics invokeThreads(ThreadHandlerContext<T> context, String msg) {
        if (context.getHandler() == null || context.getList() == null) {
            throw new IllegalArgumentException("Neither elementHandler nor list can be null");
        }
        ThreadHandler<T> handler = factory.getThreadHandler(context);
        int size = context.getList().size();
        int threads = handler.getThreadCount();
        double elementsPerThread = (size * 1D) / threads;
        logger.debug(msg + "[t:" + threads + " e:" + nf.format(elementsPerThread) + " s:" + size + "]");
        handler.executeThreads();
        return handler.getExecutionStatistics();
    }

    public <T> ExecutionStatistics invokeThreads(int max, int min, int divisor, ThreadHandlerContext<T> context) {
        context.setMax(max);
        context.setMin(min);
        context.setDivisor(divisor);
        return invokeThreads(context);
    }

    public <T> ExecutionStatistics invokeThreads(int max, ElementHandler<T> elementHandler, List<T> list) {
        ThreadHandlerContext<T> thc = new ThreadHandlerContext<T>();
        thc.setMax(max);
        thc.setHandler(elementHandler);
        thc.setList(list);
        return invokeThreads(max, 0, 0, thc);
    }

}
