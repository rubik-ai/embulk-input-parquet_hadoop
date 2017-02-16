package studio.adtech.parquet.msgpack.spark;

import org.apache.spark.sql.RowFactory;
import org.junit.Test;
import org.msgpack.value.Value;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.StringType;
import static org.apache.spark.sql.types.DataTypes.createArrayType;
import static org.apache.spark.sql.types.DataTypes.createMapType;
import static org.apache.spark.sql.types.DataTypes.createStructField;
import static org.apache.spark.sql.types.DataTypes.createStructType;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.msgpack.value.ValueFactory.newArray;
import static org.msgpack.value.ValueFactory.newInteger;
import static org.msgpack.value.ValueFactory.newMap;
import static org.msgpack.value.ValueFactory.newNil;
import static org.msgpack.value.ValueFactory.newString;

/**
 * Integration tests for compatibility with parquet files that written by spark.
 */
public class TestSparkParquet extends SparkTestBase {
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
