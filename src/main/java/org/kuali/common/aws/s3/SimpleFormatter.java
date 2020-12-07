/**
 * Copyright 2010-2012 The Kuali Foundation
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
package org.kuali.common.aws.s3;

import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Format time, bytes, counts, and transfer rate into human friendly form
 *
 * @author Jeff Caddel
 * @since May 27, 2010 6:46:17 PM
 */
public class SimpleFormatter {
    private static final double SECOND = 1000;
    private static final double MINUTE = 60 * SECOND;
    private static final double HOUR = 60 * MINUTE;
    private static final double DAY = 24 * HOUR;
    private static final double YEAR = 365 * DAY;
    private static final double DECADE = 10 * YEAR;
    private static final double CENTURY = 10 * DECADE;
    private static final String DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SZ";

    NumberFormat largeSizeFormatter = NumberFormat.getInstance();
    NumberFormat sizeFormatter = NumberFormat.getInstance();
    NumberFormat timeFormatter = NumberFormat.getInstance();
    NumberFormat rateFormatter = NumberFormat.getInstance();
    NumberFormat countFormatter = NumberFormat.getInstance();
    SimpleDateFormat dateFormatter = new SimpleDateFormat(DATE_FORMAT);

    public SimpleFormatter() {
        super();
        sizeFormatter.setGroupingUsed(false);
        sizeFormatter.setMaximumFractionDigits(1);
        sizeFormatter.setMinimumFractionDigits(1);
        largeSizeFormatter.setGroupingUsed(false);
        largeSizeFormatter.setMaximumFractionDigits(3);
        largeSizeFormatter.setMinimumFractionDigits(3);
        timeFormatter.setGroupingUsed(false);
        timeFormatter.setMaximumFractionDigits(3);
        timeFormatter.setMinimumFractionDigits(3);
        rateFormatter.setGroupingUsed(false);
        rateFormatter.setMaximumFractionDigits(3);
        rateFormatter.setMinimumFractionDigits(3);
        countFormatter.setGroupingUsed(true);
        countFormatter.setMaximumFractionDigits(0);
        countFormatter.setMinimumFractionDigits(0);
    }

    /**
     * Return a formatted date
     */
    public String getDate(long millis) {
        return getDate(new Date(millis));
    }

    /**
     * Return a formatted date
     */
    public String getDate(Date date) {
        return dateFormatter.format(date);
    }

    /**
     * Given a number of bytes and the number of milliseconds it took to transfer that number of bytes, return bytes/s, KB/s, MB/s, GB/s,
     * TB/s, PB/s, or EB/s as appropriate
     */
    public String getRate(long millis, long bytes) {
        double seconds = millis / SECOND;
        double bytesPerSecond = bytes / seconds;
        Size bandwidthLevel = getSizeEnum(bytesPerSecond);
        double transferRate = bytesPerSecond / bandwidthLevel.getValue();
        return rateFormatter.format(transferRate) + " " + bandwidthLevel.getRateLabel();
    }

    /**
     * Return a formatted <code>count</code>
     */
    public String getCount(long count) {
        return countFormatter.format(count);
    }

    /**
     * Given milliseconds, return milliseconds, seconds, minutes, hours, days, years, decades, or centuries as appropriate. Note that years,
     * decades, and centuries are approximations since the logic always assumes there are exactly 365 days per year.
     */
    public String getTime(long millis) {
        if (millis < SECOND) {
            return millis + "ms";
        } else if (millis < MINUTE) {
            return timeFormatter.format(millis / SECOND) + "s";
        } else if (millis < HOUR) {
            return timeFormatter.format(millis / MINUTE) + "m";
        } else if (millis < DAY) {
            return timeFormatter.format(millis / HOUR) + " hours";
        } else if (millis < YEAR) {
            return timeFormatter.format(millis / DAY) + " days";
        } else if (millis < DECADE) {
            return timeFormatter.format(millis / YEAR) + " years";
        } else if (millis < CENTURY) {
            return timeFormatter.format(millis / DECADE) + " decades";
        } else {
            return timeFormatter.format(millis / CENTURY) + " centuries";
        }
    }

    /**
     * Given a number of bytes return bytes, kilobytes, megabytes, gigabytes, terabytes, petabytes, or exabytes as appropriate.
     */
    public String getSize(long bytes) {
        return getSize(bytes, null);
    }

    /**
     * Given a number of bytes return bytes, kilobytes, megabytes, gigabytes, terabytes, petabytes, or exabytes as appropriate.
     */
    public String getSize(long bytes, Size size) {
        size = (size == null) ? getSizeEnum(bytes) : size;
        StringBuilder sb = new StringBuilder();
        sb.append(getFormattedSize(bytes, size));
        if (needsSpaceBeforeLabel(size)) {
            sb.append(" ");
        }
        sb.append(size.getSizeLabel());
        return sb.toString();
    }

    protected boolean needsSpaceBeforeLabel(Size size) {
        switch (size) {
            case BYTE:
            case TB:
            case PB:
            case EB:
                return true;
            default:
                return false;
        }
    }

    public String getFormattedSize(long bytes, Size size) {
        switch (size) {
            case BYTE:
                return bytes + "";
            case KB:
            case MB:
            case GB:
                return sizeFormatter.format(bytes / (double) size.getValue());
            default:
                return largeSizeFormatter.format(bytes / (double) size.getValue());
        }
    }

    public Size getSizeEnum(double bytes) {
        if (bytes < Size.KB.getValue()) {
            return Size.BYTE;
        } else if (bytes < Size.MB.getValue()) {
            return Size.KB;
        } else if (bytes < Size.GB.getValue()) {
            return Size.MB;
        } else if (bytes < Size.TB.getValue()) {
            return Size.GB;
        } else if (bytes < Size.PB.getValue()) {
            return Size.TB;
        } else if (bytes < Size.EB.getValue()) {
            return Size.PB;
        } else {
            return Size.EB;
        }
    }

}
