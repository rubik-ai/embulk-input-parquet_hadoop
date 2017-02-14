package studio.adtech.parquet.msgpack.compat;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetReader;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.msgpack.value.Value;
import studio.adtech.parquet.msgpack.CSVAsJSONIterator;
import studio.adtech.parquet.msgpack.CSVColumnWriter;
import studio.adtech.parquet.msgpack.CSVHeaderMap;
import studio.adtech.parquet.msgpack.JSONIterator;
import studio.adtech.parquet.msgpack.JSONIteratorMatcher;
import studio.adtech.parquet.msgpack.ParquetAsJSONIterator;
import studio.adtech.parquet.msgpack.read.MessagePackReadSupport;

import java.io.FileReader;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static studio.adtech.parquet.msgpack.JSONIteratorMatcher.sameAs;

@RunWith(Parameterized.class)
public class TestParquetReaderCompatibility {
    private final String parquetFilename;

    @Parameterized.Parameters
    public static List<String> data() {
        return Arrays.asList(
                "test-data/parquet-python/nation.plain.parquet",
                "test-data/parquet-python/nation.dict.parquet",
                "test-data/parquet-python/nation.impala.parquet",
                "test-data/parquet-python/snappy-nation.impala.parquet",
                "test-data/parquet-python/gzip-nation.impala.parquet"
        );
    }

    public TestParquetReaderCompatibility(String parquetFilename) {
        this.parquetFilename = parquetFilename;
    }

    @Test
    public void testing() throws Exception {
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
