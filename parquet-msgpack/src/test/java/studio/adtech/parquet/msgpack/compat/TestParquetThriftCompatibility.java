package studio.adtech.parquet.msgpack.compat;

import org.apache.commons.lang3.StringUtils;
import org.junit.Test;
import org.msgpack.value.Value;
import studio.adtech.parquet.msgpack.ParquetIterator;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.msgpack.value.ValueFactory.newArray;
import static org.msgpack.value.ValueFactory.newBoolean;
import static org.msgpack.value.ValueFactory.newFloat;
import static org.msgpack.value.ValueFactory.newInteger;
import static org.msgpack.value.ValueFactory.newMap;
import static org.msgpack.value.ValueFactory.newNil;
import static org.msgpack.value.ValueFactory.newString;

public class TestParquetThriftCompatibility {
    @Test
    public void testing() {
        ParquetIterator parquet = ParquetIterator.fromResource("test-data/spark/parquet-thrift-compat.snappy.parquet");

        String[] suits = new String[]{"SPADES", "HEARTS", "DIAMONDS", "CLUBS"};
        for (int i = 0; i < 10; i++) {
            HashMap<Value, Value> nonNullablePrimitiveValues = new HashMap<>();
            {
                HashMap<Value, Value> m = nonNullablePrimitiveValues;
                m.put(newString("boolColumn"), newBoolean(i % 2 == 0));
                m.put(newString("byteColumn"), newInteger(i));
                m.put(newString("shortColumn"), newInteger(i + 1));
                m.put(newString("intColumn"), newInteger(i + 2));
                m.put(newString("longColumn"), newInteger(i * 10));
                m.put(newString("doubleColumn"), newFloat(i + 0.2));
                // Thrift `BINARY` values are actually unencoded `STRING` values, and thus are always
                // treated as `BINARY (UTF8)` in parquet-thrift, since parquet-thrift always assume
                // Thrift `STRING`s are encoded using UTF-8.
                m.put(newString("binaryColumn"), newString("val_" + i));
                m.put(newString("stringColumn"), newString("val_" + i));
                // Thrift ENUM values are converted to Parquet binaries containing UTF-8 strings
                m.put(newString("enumColumn"), newString(suits[i % 4]));
            }

            HashMap<Value, Value> nullablePrimitiveValues = new HashMap<>();
            for (Map.Entry<Value, Value> entry : nonNullablePrimitiveValues.entrySet()) {
                Value key = newString("maybe" + StringUtils.capitalize(entry.getKey().toString()));
                Value value = (i % 3 == 0) ? newNil() : entry.getValue();
                nullablePrimitiveValues.put(key, value);
            }

            HashMap<Value, Value> complexValues = new HashMap<>();
            {
                HashMap<Value, Value> m = complexValues;
                m.put(newString("stringsColumn"), newArray(
                        newString("arr_" + i),
                        newString("arr_" + (i + 1)),
                        newString("arr_" + (i + 2))
                ));
                // Thrift `SET`s are converted to Parquet `LIST`s
                m.put(newString("intSetColumn"), newArray(newInteger(i)));
                m.put(newString("intToStringColumn"), newMap(
                        newInteger(i), newString("val_" + i),
                        newInteger(i + 1), newString("val_" + (i + 1)),
                        newInteger(i + 2), newString("val_" + (i + 2))
                ));

                m.put(newString("complexColumn"), newMap(
                        newInteger(i + 0), newComplexInnerValue(i),
                        newInteger(i + 1), newComplexInnerValue(i),
                        newInteger(i + 2), newComplexInnerValue(i)
                ));
            }

            HashMap<Value, Value> row = new HashMap<>();
            row.putAll(nonNullablePrimitiveValues);
            row.putAll(nullablePrimitiveValues);
            row.putAll(complexValues);

            Value expected = newMap(row);
            Value actual = parquet.next();
            assertThat(actual, is(expected));
        }
    }

    private Value newComplexInnerValue(int i) {
        return newArray(
                newMap(
                        newString("nestedIntsColumn"), newArray(newInteger(i + 0 + 0), newInteger(i + 0 + 1), newInteger(i + 0 + 2)),
                        newString("nestedStringColumn"), newString("val_" + (i + 0))
                ),
                newMap(
                        newString("nestedIntsColumn"), newArray(newInteger(i + 1 + 0), newInteger(i + 1 + 1), newInteger(i + 1 + 2)),
                        newString("nestedStringColumn"), newString("val_" + (i + 1))
                ),
                newMap(
                        newString("nestedIntsColumn"), newArray(newInteger(i + 2 + 0), newInteger(i + 2 + 1), newInteger(i + 2 + 2)),
                        newString("nestedStringColumn"), newString("val_" + (i + 2))
                )
        );
    }
}
