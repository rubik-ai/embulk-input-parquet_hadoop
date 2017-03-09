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

import jp.co.cyberagent.parquet.msgpack.JSONIterator;
import jp.co.cyberagent.parquet.msgpack.ParquetAsJSONIterator;
import org.junit.Test;
import org.skyscreamer.jsonassert.JSONAssert;

/**
 * Test the DefinitionLevel handling.
 *
 * @see https://github.com/jcrobak/parquet-python/blob/45165f3159505524d708894337e68120fcd844e7/test/test_read_support.py#L207
 */
public class TestParquetPythonDefinitionLevel
{
    @Test
    public void testReadingAFileThatContainsNullRecords() throws Exception
    {
        JSONIterator parquet = ParquetAsJSONIterator.fromResource(
                "test-data/parquet-python/test-null.parquet");

        JSONAssert.assertEquals(parquet.next(), "{\"foo\":1,\"bar\":2}", true);
        JSONAssert.assertEquals(parquet.next(), "{\"foo\":1,\"bar\":null}", true);
    }

    @Test
    public void testReadingAFileThatContainsNullRecordsForAPlainColumnThatIsConvertedToUTF8() throws Exception
    {
        JSONIterator parquet = ParquetAsJSONIterator.fromResource(
                "test-data/parquet-python/test-converted-type-null.parquet");

        JSONAssert.assertEquals(parquet.next(), "{\"foo\":\"bar\"}", true);
        JSONAssert.assertEquals(parquet.next(), "{\"foo\":null}", true);
    }

    @Test
    public void testReadingAFileThatContainsNullRecordsForAPlainDictionaryColumn() throws Exception
    {
        JSONIterator parquet = ParquetAsJSONIterator.fromResource(
                "test-data/parquet-python/test-null-dictionary.parquet");

        JSONAssert.assertEquals(parquet.next(), "{\"foo\":null}", true);
        JSONAssert.assertEquals(parquet.next(), "{\"foo\":\"bar\"}", true);
        JSONAssert.assertEquals(parquet.next(), "{\"foo\":\"baz\"}", true);
        JSONAssert.assertEquals(parquet.next(), "{\"foo\":\"bar\"}", true);
        JSONAssert.assertEquals(parquet.next(), "{\"foo\":\"baz\"}", true);
        JSONAssert.assertEquals(parquet.next(), "{\"foo\":\"bar\"}", true);
        JSONAssert.assertEquals(parquet.next(), "{\"foo\":\"baz\"}", true);
    }
}
