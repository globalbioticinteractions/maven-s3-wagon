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

import java.util.List;

import org.kuali.common.threads.listener.NoOpListener;
import org.kuali.common.threads.listener.ProgressListener;

/**
 * Provides context for a ThreadHandler
 *
 * @param <T>
 */
public class ThreadHandlerContext<T> {
    public static final int DEFAULT_MAX_THREADS = 1;

    // Min threads to use
    int min;

    // Max threads to use
    int max = DEFAULT_MAX_THREADS;

    // If supplied, divide # of elements in the list by this number to come up with a thread count
    // Thread count will always be constrained by max regardless. This gives clients a way to
    // scale threads up relative to the number of items in the list
    int divisor;

    // The list of elements to iterate over
    List<T> list;

    // The handler for dealing with an item from the list
    ElementHandler<T> handler;

    // By default don't do anything when informed about progress being made
    ProgressListener<T> listener = new NoOpListener<T>();

    public int getMin() {
        return min;
    }

    public void setMin(int min) {
        this.min = min;
    }

    public int getMax() {
        return max;
    }

    public void setMax(int max) {
        this.max = max;
    }

    public List<T> getList() {
        return list;
    }

    public void setList(List<T> list) {
        this.list = list;
    }

    public ElementHandler<T> getHandler() {
        return handler;
    }

    public void setHandler(ElementHandler<T> handler) {
        this.handler = handler;
    }

    public ProgressListener<T> getListener() {
        return listener;
    }

    public void setListener(ProgressListener<T> listener) {
        this.listener = listener;
    }

    public int getDivisor() {
        return divisor;
    }

    public void setDivisor(int divisor) {
        this.divisor = divisor;
    }

}
