/*
 * This class includes code and test data from Apache Spark.
 *
 * This class includes code and test data from parquet-cpp.
 *     https://github.com/apache/parquet-cpp
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
package studio.adtech.parquet.msgpack.compat;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import studio.adtech.parquet.msgpack.JSONIterator;
import studio.adtech.parquet.msgpack.JSONLinesIterator;
import studio.adtech.parquet.msgpack.ParquetAsJSONIterator;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static studio.adtech.parquet.msgpack.JSONIteratorMatcher.sameAs;

@RunWith(Parameterized.class)
public class TestParquetCompatibilityAgainstJson
{
    private final String parquetFilename;

    @Parameterized.Parameters
    public static List<String> data()
    {
        return Arrays.asList(
                "test-data/parquet-cpp/alltypes_plain.parquet",
                "test-data/parquet-cpp/alltypes_plain.snappy.parquet",
                "test-data/parquet-cpp/alltypes_dictionary.parquet",
                "test-data/parquet-cpp/nation.dict-malformed.parquet",

                // unannotated array of primitive type
                "test-data/spark/old-repeated-int.parquet",

                // unannotated array of struct
                "test-data/spark/old-repeated-message.parquet",
                "test-data/spark/proto-repeated-struct.parquet",
                "test-data/spark/proto-struct-with-array-many.parquet",

                // struct with unannotated array
                "test-data/spark/proto-struct-with-array.parquet",

                // unannotated array of string
                "test-data/spark/proto-repeated-string.parquet"
        );
    }

    public TestParquetCompatibilityAgainstJson(String parquetFilename)
    {
        this.parquetFilename = parquetFilename;
    }

    @Test
    public void testing()
    {
        JSONIterator parquet = ParquetAsJSONIterator.fromResource(parquetFilename);

        String jsonName = parquetFilename.replaceFirst("\\.parquet$", ".jsonl");
        JSONIterator expected = JSONLinesIterator.fromResource(jsonName);

        assertThat(parquet, is(sameAs(expected)));
    }
}
