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
