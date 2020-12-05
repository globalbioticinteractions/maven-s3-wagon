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
package org.kuali.common.threads.listener;

/**
 * Keeps track of overall progress towards a goal.
 *
 * Notifies its listener whenever progress has occurred.
 *
 * Also notifies the listener when progress starts and completes.
 */
public class ProgressNotifier<T> {

    ProgressListener<T> listener = new NoOpListener<T>();
    int progress;
    int total;

    public synchronized int getProgress() {
        return progress;
    }

    public synchronized void notify(ProgressEvent<T> event) {
        if (progress == 0) {
            listener.progressStarted();
        }
        progress++;
        listener.progressOccurred(progress, total, event);
        if (progress == total) {
            listener.progressCompleted();
        }
    }

    public ProgressListener<T> getListener() {
        return listener;
    }

    public void setListener(ProgressListener<T> listener) {
        this.listener = listener;
    }

    public int getTotal() {
        return total;
    }

    public void setTotal(int total) {
        this.total = total;
    }

}
