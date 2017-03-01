/*
 * This class includes code from Apache Spark.
 *
 * Copyright 2017 CyberAgent, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package studio.adtech.parquet.msgpack.read.converter;

public abstract class DateTimeUtils {
    // see http://stackoverflow.com/questions/466321/convert-unix-timestamp-to-julian
    // it's 2440587.5, rounding up to compatible with Hive
    private static final int JULIAN_DAY_OF_EPOCH = 2440588;
    private static final long SECONDS_PER_DAY = 60 * 60 * 24L;
    private static final long MICROS_PER_SECOND = 1000L * 1000L;

    public static long fromJulianDay(int day, long nanoseconds) {
        // use Long to avoid rounding errors
        long seconds = (long) (day - JULIAN_DAY_OF_EPOCH) * SECONDS_PER_DAY;
        return seconds * MICROS_PER_SECOND + nanoseconds / 1000L;
    }
}
