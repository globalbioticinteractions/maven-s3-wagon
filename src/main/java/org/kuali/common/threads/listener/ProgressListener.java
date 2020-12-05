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
 * Listen for events related to making progress towards an overall total.
 *
 * @param <T>
 */
public interface ProgressListener<T> {

    /**
     * Progress has begun
     */
    void progressStarted();

    /**
     * Progress towards the total has occurred
     *
     * @param count
     * @param total
     * @param event
     */
    void progressOccurred(int count, int total, ProgressEvent<T> event);

    /**
     * Progress is now complete
     */
    void progressCompleted();
}
