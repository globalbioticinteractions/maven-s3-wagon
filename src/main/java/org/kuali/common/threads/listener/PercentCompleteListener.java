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
 * Print a dot whenever there is progress of 2% or more
 */
public class PercentCompleteListener<T> extends MavenConsoleListener<T> {
    int percentageIncrement = 2;
    int percentCompletePrevious;

    @Override
    public void progressOccurred(int count, int total, ProgressEvent<T> event) {
        int percentComplete = (count * 100) / total;
        if (enoughProgress(percentComplete)) {
            percentCompletePrevious = percentComplete;
            out.print(progressToken);
        }
    }

    protected boolean enoughProgress(int percentComplete) {
        int needed = percentCompletePrevious + percentageIncrement;
        return percentComplete >= needed;
    }

    public int getPercentageIncrement() {
        return percentageIncrement;
    }

    public void setPercentageIncrement(int percentageIncrement) {
        this.percentageIncrement = percentageIncrement;
    }

}
