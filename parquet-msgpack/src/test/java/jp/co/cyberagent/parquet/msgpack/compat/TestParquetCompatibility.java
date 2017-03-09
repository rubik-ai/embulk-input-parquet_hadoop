/*
 * This class includes code and test data from parquet-python.
 *     https://github.com/jcrobak/parquet-python
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
package jp.co.cyberagent.parquet.msgpack.compat;

import jp.co.cyberagent.parquet.msgpack.CSVAsJSONIterator;
import jp.co.cyberagent.parquet.msgpack.CSVColumnWriter;
import jp.co.cyberagent.parquet.msgpack.CSVHeaderMap;
import jp.co.cyberagent.parquet.msgpack.JSONIterator;
import jp.co.cyberagent.parquet.msgpack.ParquetAsJSONIterator;
import org.apache.commons.csv.CSVFormat;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.List;

import static jp.co.cyberagent.parquet.msgpack.JSONIteratorMatcher.sameAs;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * Integration tests for compatibility with reference parquet files.
 *
 * @see https://github.com/jcrobak/parquet-python/blob/45165f3159505524d708894337e68120fcd844e7/test/test_read_support.py#L109
 */
@RunWith(Parameterized.class)
public class TestParquetCompatibility
{
    private final String parquetFilename;

    @Parameterized.Parameters
    public static List<String> data()
    {
        return Arrays.asList(
                "test-data/parquet-python/nation.plain.parquet",
                "test-data/parquet-python/nation.dict.parquet",
                "test-data/parquet-python/nation.impala.parquet",
                "test-data/parquet-python/snappy-nation.impala.parquet",
                "test-data/parquet-python/gzip-nation.impala.parquet"
        );
    }

    public TestParquetCompatibility(String parquetFilename)
    {
        this.parquetFilename = parquetFilename;
    }

    @Test
    public void testing() throws Exception
    {
        JSONIterator parquet = ParquetAsJSONIterator.fromResource(parquetFilename);

        boolean isImpala = parquetFilename.contains("impala");

        CSVHeaderMap headerMap = CSVHeaderMap.builder()
                .add(isImpala ? "n_nationkey" : "nation_key", CSVColumnWriter.NUMBER)
                .add(isImpala ? "n_name" : "name", CSVColumnWriter.STRING)
                .add(isImpala ? "n_regionkey" : "region_key", CSVColumnWriter.NUMBER)
                .add(isImpala ? "n_comment" : "comment_col", CSVColumnWriter.STRING)
                .build();
        CSVFormat format = CSVFormat.newFormat('|');
        JSONIterator csv = CSVAsJSONIterator.fromResource("test-data/parquet-python/nation.csv", format, headerMap);

        assertThat(parquet, is(sameAs(csv)));
    }
}
