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

import org.kuali.common.threads.listener.ProgressNotifier;

/**
 * Provides context for iterating over the elements in a list. Offset and length control what portion of the list is
 * iterated over. The ElementHandler is invoked each time an element in the list is encountered.
 *
 * @param <T>
 */
public class ListIteratorContext<T> {
    int id;
    int offset;
    int length;
    ThreadHandler<T> threadHandler;
    ProgressNotifier<T> notifier;
    List<T> list;
    ElementHandler<T> elementHandler;

    public ListIteratorContext() {
        this(0, 0, 0, null);
    }

    public ListIteratorContext(int id, int offset, int length, List<T> list) {
        super();
        this.id = id;
        this.offset = offset;
        this.length = length;
        this.list = list;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getOffset() {
        return offset;
    }

    public void setOffset(int offset) {
        this.offset = offset;
    }

    public int getLength() {
        return length;
    }

    public void setLength(int length) {
        this.length = length;
    }

    public ThreadHandler<T> getThreadHandler() {
        return threadHandler;
    }

    public void setThreadHandler(ThreadHandler<T> threadHandler) {
        this.threadHandler = threadHandler;
    }

    public ProgressNotifier<T> getNotifier() {
        return notifier;
    }

    public void setNotifier(ProgressNotifier<T> tracker) {
        this.notifier = tracker;
    }

    public List<T> getList() {
        return list;
    }

    public void setList(List<T> list) {
        this.list = list;
    }

    public ElementHandler<T> getElementHandler() {
        return elementHandler;
    }

    public void setElementHandler(ElementHandler<T> elementHandler) {
        this.elementHandler = elementHandler;
    }

}
