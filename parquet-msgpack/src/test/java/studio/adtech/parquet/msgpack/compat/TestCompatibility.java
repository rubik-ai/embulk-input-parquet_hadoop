package studio.adtech.parquet.msgpack.compat;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetReader;
import org.junit.Test;
import org.msgpack.jackson.dataformat.MessagePackFactory;
import org.msgpack.value.Value;
import studio.adtech.parquet.msgpack.read.MessagePackReadSupport;

public class TestCompatibility {
    @Test
    public void test1() throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

        String path = getClass().getClassLoader().getResource("test-data/parquet-python/nation.plain.parquet").getPath();
        ParquetReader<Value> reader = ParquetReader.builder(new MessagePackReadSupport(), new Path(path))
                .withConf(conf)
                .build();
        for (Value value = reader.read(); value != null; value = reader.read()) {
            System.out.println(value.toString());
        }
        reader.close();
    }
}
