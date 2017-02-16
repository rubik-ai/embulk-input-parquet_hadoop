package studio.adtech.parquet.msgpack.spark;

import org.apache.spark.sql.RowFactory;
import org.junit.Test;
import org.msgpack.value.Value;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import static org.apache.spark.sql.types.DataTypes.BinaryType;
import static org.apache.spark.sql.types.DataTypes.BooleanType;
import static org.apache.spark.sql.types.DataTypes.ByteType;
import static org.apache.spark.sql.types.DataTypes.DateType;
import static org.apache.spark.sql.types.DataTypes.DoubleType;
import static org.apache.spark.sql.types.DataTypes.FloatType;
import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.LongType;
import static org.apache.spark.sql.types.DataTypes.ShortType;
import static org.apache.spark.sql.types.DataTypes.StringType;
import static org.apache.spark.sql.types.DataTypes.TimestampType;
import static org.apache.spark.sql.types.DataTypes.createArrayType;
import static org.apache.spark.sql.types.DataTypes.createDecimalType;
import static org.apache.spark.sql.types.DataTypes.createMapType;
import static org.apache.spark.sql.types.DataTypes.createStructField;
import static org.apache.spark.sql.types.DataTypes.createStructType;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.msgpack.value.ValueFactory.newArray;
import static org.msgpack.value.ValueFactory.newBinary;
import static org.msgpack.value.ValueFactory.newBoolean;
import static org.msgpack.value.ValueFactory.newFloat;
import static org.msgpack.value.ValueFactory.newInteger;
import static org.msgpack.value.ValueFactory.newMap;
import static org.msgpack.value.ValueFactory.newNil;
import static org.msgpack.value.ValueFactory.newString;

/**
 * Integration tests for compatibility with parquet files that written by spark.
 */
public class TestSparkParquet extends SparkTestBase {
    @Test
    public void testPrimitives() throws Exception {
        /*
        message spark_schema {
          optional binary string (UTF8);
          optional binary binary;
          optional boolean boolean;
          optional int32 date (DATE);
          optional int96 timestamp;
          optional double double;
          optional float float;
          optional int32 byte (INT_8);
          optional int32 integer;
          optional int64 long;
          optional int32 short (INT_16);
        }
         */
        List<Value> actual = parquet("test-primitives")
                .withSchema(
                        createStructField("string", StringType, true),
                        createStructField("binary", BinaryType, true),
                        createStructField("boolean", BooleanType, true),
                        createStructField("date", DateType, true),
                        createStructField("timestamp", TimestampType, true),
                        createStructField("double", DoubleType, true),
                        createStructField("float", FloatType, true),
                        createStructField("byte", ByteType, true),
                        createStructField("integer", IntegerType, true),
                        createStructField("long", LongType, true),
                        createStructField("short", ShortType, true)
                )
                .withData(
                        RowFactory.create("foo", new byte[] { 0x20 }, true, new java.sql.Date(0), new java.sql.Timestamp(1), 1.5d, 2.5f, (byte)3, 4, 5L, (short)6),
                        RowFactory.create(null, null, null, null, null, null, null, null, null, null, null)
                )
                .read();

        Value expected0 = newMap(
                newString("string"), newString("foo"),
                newString("binary"), newBinary(new byte[] { 0x20 }),
                newString("boolean"), newBoolean(true),
                newString("date"), newInteger(0), // days from 1970-01-01
                newString("timestamp"), newInteger(1000),
                newString("double"), newFloat(1.5),
                newString("float"), newFloat(2.5),
                newString("byte"), newInteger(3),
                newString("integer"), newInteger(4),
                newString("long"), newInteger(5),
                newString("short"), newInteger(6)
        );
        assertThat(actual.get(0), is(expected0));

        Value expected1 = newMap(
                newString("string"), newNil(),
                newString("binary"), newNil(),
                newString("boolean"), newNil(),
                newString("date"), newNil(),
                newString("timestamp"), newNil(),
                newString("double"), newNil(),
                newString("float"), newNil(),
                newString("byte"), newNil(),
                newString("integer"), newNil(),
                newString("long"), newNil(),
                newString("short"), newNil()
        );
        assertThat(actual.get(1), is(expected1));
    }

    @Test
    public void testDecimal() throws Exception {
        /*
        message spark_schema {
          optional int32 d_int32 (DECIMAL(9,3));
          optional int64 d_int64 (DECIMAL(18,3));
          optional fixed_len_byte_array(9) d_binary (DECIMAL(20,3));
        }
         */
        List<Value> actual = parquet("test-decimal")
                .withSchema(
                        createStructField("d_int32", createDecimalType(9, 3), true),
                        createStructField("d_int64", createDecimalType(18, 3), true),
                        createStructField("d_binary", createDecimalType(20, 3), true)
                )
                .withData(
                        RowFactory.create(new BigDecimal("1.333"), new BigDecimal("1.333"), new BigDecimal("1.333")),
                        RowFactory.create(null, null, null)
                )
                .read();

        Value expected0 = newMap(
                newString("d_int32"), newFloat(1.333),
                newString("d_int64"), newFloat(1.333),
                newString("d_binary"), newFloat(1.333)
        );
        assertThat(actual.get(0), is(expected0));

        Value expected1 = newMap(
                newString("d_int32"), newNil(),
                newString("d_int64"), newNil(),
                newString("d_binary"), newNil()
        );
        assertThat(actual.get(1), is(expected1));
    }

    @Test
    public void testDecimalLegacy() throws Exception {
        /*
        message spark_schema {
          optional fixed_len_byte_array(5) column (DECIMAL(10,3));
        }
         */
        List<Value> actual = parquet("test-decimal-legacy")
                .withSchema(
                        createStructField("column", createDecimalType(10, 3), true)
                )
                .withData(
                        RowFactory.create(new BigDecimal("1.333")),
                        RowFactory.create(new Object[] { null })
                )
                .withLegacyFormat()
                .read();

        Value expected0 = newMap(
                newString("column"), newFloat(1.333)
        );
        assertThat(actual.get(0), is(expected0));

        Value expected1 = newMap(
                newString("column"), newNil()
        );
        assertThat(actual.get(1), is(expected1));
    }

    @Test
    public void testMapStringKey() throws Exception {
        /*
        message spark_schema {
          optional group column (MAP) {
            repeated group key_value {
              required binary key (UTF8);
              optional int32 value;
            }
          }
        }
         */
        List<Value> actual = parquet("test-map-string-key")
                .withSchema(
                        createStructField("column", createMapType(StringType, IntegerType, true), true)
                )
                .withData(
                        RowFactory.create(new HashMap<String, Integer>(){{
                            put("foo", 1);
                            put("bar", 2);
                        }}),
                        RowFactory.create(new HashMap<String, Integer>(){{
                            put("baz", null);
                        }}),
                        RowFactory.create((Object)null)
                )
                .read();

        Value expected0 = newMap(
                newString("column"), newMap(
                        newString("foo"), newInteger(1),
                        newString("bar"), newInteger(2)
                )
        );
        assertThat(actual.get(0), is(expected0));

        Value expected1 = newMap(
                newString("column"), newMap(
                        newString("baz"), newNil()
                )
        );
        assertThat(actual.get(1), is(expected1));

        Value expected2 = newMap(
                newString("column"), newNil()
        );
        assertThat(actual.get(2), is(expected2));
    }

    @Test
    public void testMapIntegerKey() throws Exception {
        /*
        message spark_schema {
          optional group column (MAP) {
            repeated group key_value {
              required binary key (UTF8);
              optional int32 value;
            }
          }
        }
         */
        List<Value> actual = parquet("test-map-integer-key")
                .withSchema(
                        createStructField("column", createMapType(IntegerType, IntegerType, true), true)
                )
                .withData(
                        RowFactory.create(new HashMap<Integer, Integer>(){{
                            put(1, 100);
                            put(2, 200);
                        }})
                )
                .read();

        Value expected0 = newMap(
                newString("column"), newMap(
                        newInteger(1), newInteger(100),
                        newInteger(2), newInteger(200)
                )
        );
        assertThat(actual.get(0), is(expected0));
    }

    @Test
    public void testMapLegacy() throws Exception {
        /*
        message spark_schema {
          optional group column (MAP) {
            repeated group map (MAP_KEY_VALUE) {
              required binary key (UTF8);
              optional int32 value;
            }
          }
        }
         */
        List<Value> actual = parquet("test-map-legacy")
                .withSchema(
                        createStructField("column", createMapType(StringType, IntegerType, true), true)
                )
                .withData(
                        RowFactory.create(new HashMap<String, Integer>(){{
                            put("foo", 1);
                            put("bar", 2);
                        }}),
                        RowFactory.create(new HashMap<String, Integer>(){{
                            put("baz", null);
                        }}),
                        RowFactory.create((Object)null)
                )
                .withLegacyFormat()
                .read();

        Value expected0 = newMap(
                newString("column"), newMap(
                        newString("foo"), newInteger(1),
                        newString("bar"), newInteger(2)
                )
        );
        assertThat(actual.get(0), is(expected0));

        Value expected1 = newMap(
                newString("column"), newMap(
                        newString("baz"), newNil()
                )
        );
        assertThat(actual.get(1), is(expected1));

        Value expected2 = newMap(
                newString("column"), newNil()
        );
        assertThat(actual.get(2), is(expected2));
    }

    @Test
    public void testNestedStruct() throws Exception {
        /*
        message spark_schema {
          optional group column {
            optional int32 inner;
          }
        }
         */
        List<Value> actual = parquet("test-nested-struct")
                .withSchema(
                        createStructField("column", createStructType(Arrays.asList(
                                createStructField("inner", IntegerType, true)
                        )), true)
                )
                .withData(
                        RowFactory.create(RowFactory.create(123)),
                        RowFactory.create(RowFactory.create((Object)null)),
                        RowFactory.create((Object)null)
                )
                .read();


        Value expected0 = newMap(
                newString("column"), newMap(
                        newString("inner"), newInteger(123)
                )
        );
        assertThat(actual.get(0), is(expected0));

        Value expected1 = newMap(
                newString("column"), newMap(
                        newString("inner"), newNil()
                )
        );
        assertThat(actual.get(1), is(expected1));

        Value expected2 = newMap(
                newString("column"), newNil()
        );
        assertThat(actual.get(2), is(expected2));
    }

    @Test
    public void testArray() throws Exception {
        /*
        message spark_schema {
          optional group column (LIST) {
            repeated group list {
              optional int32 element;
            }
          }
        }
         */
        List<Value> actual = parquet("test-array")
                .withSchema(
                        createStructField("column", createArrayType(IntegerType, true), true)
                )
                .withData(
                        RowFactory.create((Object)new Integer[] { 1, 2 }),
                        RowFactory.create((Object)new Integer[] { null, null }),
                        RowFactory.create((Object)null)
                )
                .read();


        Value expected0 = newMap(
                newString("column"), newArray(newInteger(1), newInteger(2))
        );
        assertThat(actual.get(0), is(expected0));

        Value expected1 = newMap(
                newString("column"), newArray(newNil(), newNil())
        );
        assertThat(actual.get(1), is(expected1));

        Value expected2 = newMap(
                newString("column"), newNil()
        );
        assertThat(actual.get(2), is(expected2));
    }

    @Test
    public void testArrayLegacy() throws Exception {
        /*
        message spark_schema {
          optional group column (LIST) {
            repeated group bag {
              optional int32 array;
            }
          }
        }
         */
        List<Value> actual = parquet("test-array-legacy")
                .withSchema(
                        createStructField("column", createArrayType(IntegerType, true), true)
                )
                .withData(
                        RowFactory.create((Object)new Integer[] { 1, 2 }),
                        RowFactory.create((Object)new Integer[] { null, null }),
                        RowFactory.create((Object)null)
                )
                .withLegacyFormat()
                .read();


        Value expected0 = newMap(
                newString("column"), newArray(newInteger(1), newInteger(2))
        );
        assertThat(actual.get(0), is(expected0));

        Value expected1 = newMap(
                newString("column"), newArray(newNil(), newNil())
        );
        assertThat(actual.get(1), is(expected1));

        Value expected2 = newMap(
                newString("column"), newNil()
        );
        assertThat(actual.get(2), is(expected2));
    }

    @Test
    public void testArrayLegacy2Level() throws Exception {
        /*
        message spark_schema {
          optional group column (LIST) {
            repeated int32 array;
          }
        }
         */
        List<Value> actual = parquet("test-array-legacy-2level")
                .withSchema(
                        createStructField("column", createArrayType(IntegerType, false), true)
                )
                .withData(
                        RowFactory.create((Object)new Integer[] { 1, 2 }),
                        RowFactory.create((Object)null)
                )
                .withLegacyFormat()
                .read();


        Value expected0 = newMap(
                newString("column"), newArray(newInteger(1), newInteger(2))
        );
        assertThat(actual.get(0), is(expected0));

        Value expected1 = newMap(
                newString("column"), newNil()
        );
        assertThat(actual.get(1), is(expected1));
    }
}
