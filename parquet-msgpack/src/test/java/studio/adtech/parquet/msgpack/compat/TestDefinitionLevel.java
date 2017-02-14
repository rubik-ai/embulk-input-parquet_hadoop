package studio.adtech.parquet.msgpack.compat;

import org.junit.Test;
import org.skyscreamer.jsonassert.JSONAssert;
import studio.adtech.parquet.msgpack.JSONIterator;
import studio.adtech.parquet.msgpack.ParquetAsJSONIterator;

/**
 * Test the DefinitionLevel handling.
 *
 * @see https://github.com/jcrobak/parquet-python/blob/45165f3159505524d708894337e68120fcd844e7/test/test_read_support.py#L207
 */
public class TestDefinitionLevel {
    @Test
    public void testReadingAFileThatContainsNullRecords() throws Exception {
        JSONIterator parquet = ParquetAsJSONIterator.fromResource(
                "test-data/parquet-python/test-null.parquet");

        JSONAssert.assertEquals(parquet.next(), "{\"foo\":1,\"bar\":2}", true);
        JSONAssert.assertEquals(parquet.next(), "{\"foo\":1,\"bar\":null}", true);
    }

    @Test
    public void testReadingAFileThatContainsNullRecordsForAPlainColumnThatIsConvertedToUTF8() throws Exception {
        JSONIterator parquet = ParquetAsJSONIterator.fromResource(
                "test-data/parquet-python/test-converted-type-null.parquet");

        JSONAssert.assertEquals(parquet.next(), "{\"foo\":\"bar\"}", true);
        JSONAssert.assertEquals(parquet.next(), "{\"foo\":null}", true);
    }

    @Test
    public void testReadingAFileThatContainsNullRecordsForAPlainDictionaryColumn() throws Exception {
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
