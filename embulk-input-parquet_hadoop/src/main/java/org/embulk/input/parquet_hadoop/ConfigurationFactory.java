/*
 * This class includes code from embulk-input-hadoop.
 *   (https://github.com/civitaspo/embulk-input-hdfs)
 *
 * The MIT License
 * Copyright (c) 2015 Civitaspo
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package org.embulk.input.parquet_hadoop;

import org.apache.hadoop.conf.Configuration;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigException;
import org.embulk.spi.Exec;
import org.slf4j.Logger;

import java.io.File;
import java.net.MalformedURLException;
import java.util.List;
import java.util.Map;

public class ConfigurationFactory
{
    private static final Logger logger = Exec.getLogger(ConfigurationFactory.class);

    interface Task
    {
        @Config("config_files")
        @ConfigDefault("[]")
        List<String> getConfigFiles();

        @Config("config")
        @ConfigDefault("{}")
        Map<String, String> getConfig();
    }

    private ConfigurationFactory()
    {
    }

    public static Configuration create(Task task)
    {
        Configuration c = new Configuration();
        for (String f : task.getConfigFiles()) {
            try {
                logger.trace("embulk-input-parquet_hadoop: load a config file: {}", f);
                c.addResource(new File(f).toURI().toURL());
            }
            catch (MalformedURLException e) {
                throw new ConfigException(e);
            }
        }

        for (Map.Entry<String, String> entry : task.getConfig().entrySet()) {
            logger.trace("embulk-input-parquet_hadoop: load a config: {}:{}", entry.getKey(), entry.getValue());
            c.set(entry.getKey(), entry.getValue());
        }

        // For logging
        for (Map.Entry<String, String> entry : c) {
            logger.trace("embulk-input-parquet_hadoop: loaded: {}: {}", entry.getKey(), entry.getValue());
        }
        logger.trace("embulk-input-parquet_hadoop: loaded files: {}", c);

        return c;
    }
}
