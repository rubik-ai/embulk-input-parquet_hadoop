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
public class TestParquetCppCompatibility {
    private final String parquetFilename;

    @Parameterized.Parameters
    public static List<String> data() {
        return Arrays.asList(
                "test-data/parquet-cpp/alltypes_plain.parquet",
                "test-data/parquet-cpp/alltypes_plain.snappy.parquet",
                "test-data/parquet-cpp/alltypes_dictionary.parquet",
                "test-data/parquet-cpp/nation.dict-malformed.parquet"
        );
    }

    public TestParquetCppCompatibility(String parquetFilename) {
        this.parquetFilename = parquetFilename;
    }

    @Test
    public void testing() {
        JSONIterator parquet = ParquetAsJSONIterator.fromResource(parquetFilename);

        String jsonName = parquetFilename.replaceFirst("\\.parquet$", ".jsonl");
        JSONIterator expected = JSONLinesIterator.fromResource(jsonName);

        assertThat(parquet, is(sameAs(expected)));
    }
}
