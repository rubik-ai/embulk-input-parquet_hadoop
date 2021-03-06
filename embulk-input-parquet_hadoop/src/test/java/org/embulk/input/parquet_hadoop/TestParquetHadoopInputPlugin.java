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
package org.embulk.input.parquet_hadoop;

import com.google.common.io.Resources;
import org.embulk.config.ConfigSource;
import org.embulk.spi.InputPlugin;
import org.embulk.test.TestingEmbulk;
import org.junit.Rule;
import org.junit.Test;

import java.nio.file.Path;

import static org.embulk.test.EmbulkTests.readFile;
import static org.embulk.test.EmbulkTests.readResource;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class TestParquetHadoopInputPlugin
{
    private static final String RESOURCE_NAME_PREFIX = "test-data/";

    @Rule
    public TestingEmbulk embulk = TestingEmbulk.builder()
            .registerPlugin(InputPlugin.class, "parquet_hadoop", ParquetHadoopInputPlugin.class)
            .build();

    @Test
    public void testSimple() throws Exception
    {
        assertRecordsByResource(embulk, "simple/in.yml", "simple/data.parquet",
                "simple/expected.csv");
    }

    @Test
    public void testIncompatibleSchema() throws Exception
    {
        assertRecordsByResource(embulk, "incompatible-schema/in.yml", "incompatible-schema/data",
                "incompatible-schema/expected.csv");
    }

    static void assertRecordsByResource(TestingEmbulk embulk,
                                        String inConfigYamlResourceName,
                                        String sourceResourceName, String resultCsvResourceName)
            throws Exception
    {
        Path outputPath = embulk.createTempFile("csv");

        // in: config
        String inputPath = Resources.getResource(RESOURCE_NAME_PREFIX + sourceResourceName).toURI().toString();
        ConfigSource inConfig = embulk.loadYamlResource(RESOURCE_NAME_PREFIX + inConfigYamlResourceName)
                .set("path", inputPath);

        TestingEmbulk.RunResult result = embulk.inputBuilder()
                .in(inConfig)
                .outputPath(outputPath)
                .run();

        assertThat(readFile(outputPath), is(readResource(RESOURCE_NAME_PREFIX + resultCsvResourceName)));
    }
}
