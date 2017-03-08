/*
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
package studio.adtech.parquet.msgpack.spark;

import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.internal.SQLConf$;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.msgpack.value.Value;
import studio.adtech.parquet.msgpack.read.MessagePackReadSupport;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class SparkTestBase
{
    protected static SparkSession spark;

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @BeforeClass
    public static void beforeAll()
    {
        spark = SparkSession.builder()
                .master("local[2]")
                .appName("test")
                .getOrCreate();
    }

    @AfterClass
    public static void afterAll()
    {
        if (spark != null) {
            spark.stop();
            spark = null;
        }
    }

    protected ParquetVerifier parquet(String name)
    {
        return new ParquetVerifier(name);
    }

    protected class ParquetVerifier
    {
        private final String name;
        private StructType schema;
        private List<Row> data;
        private Map<String, String> options = new HashMap<>();
        private boolean isLegacyFormat = false;

        public ParquetVerifier(String name)
        {
            this.name = name;
        }

        public ParquetVerifier withSchema(StructField... fields)
        {
            this.schema = new StructType(fields);
            return this;
        }

        public ParquetVerifier withSchema(StructType schema)
        {
            this.schema = schema;
            return this;
        }

        public ParquetVerifier withData(List<Row> data)
        {
            this.data = data;
            return this;
        }

        public ParquetVerifier withData(Row... data)
        {
            this.data = Arrays.asList(data);
            return this;
        }

        public ParquetVerifier withLegacyFormat()
        {
            this.options.put(SQLConf$.MODULE$.PARQUET_WRITE_LEGACY_FORMAT().key(), "true");
            this.isLegacyFormat = true;
            return this;
        }

        public List<Value> read() throws IOException
        {
            spark.conf().set(SQLConf$.MODULE$.PARQUET_WRITE_LEGACY_FORMAT().key(), isLegacyFormat);

            Dataset<Row> dataFrame = spark.createDataFrame(data, schema).repartition(1);
            File file = new File(SparkTestBase.this.tempFolder.getRoot(), name);
            dataFrame.write().options(options).parquet(file.getPath());

            ArrayList<Value> results = new ArrayList<>();
            try (ParquetReader<Value> reader = ParquetReader
                    .builder(new MessagePackReadSupport(), new Path(file.getPath()))
                    .build()) {
                Value v;
                while ((v = reader.read()) != null) {
                    results.add(v);
                }
            }
            return results;
        }
    }
}
