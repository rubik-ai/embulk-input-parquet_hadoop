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

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathNotFoundException;
import org.apache.parquet.ParquetRuntimeException;
import org.apache.parquet.hadoop.util.HiddenFileFilter;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.spi.Column;
import org.embulk.spi.DataException;
import org.embulk.spi.Exec;
import org.embulk.spi.InputPlugin;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageOutput;
import org.embulk.spi.Schema;
import org.embulk.spi.type.Types;
import org.msgpack.value.Value;
import org.slf4j.Logger;
import org.slf4j.bridge.SLF4JBridgeHandler;
import studio.adtech.parquet.msgpack.read.MessagePackReadSupport;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;
import java.util.logging.Level;

public class ParquetHadoopInputPlugin
        implements InputPlugin
{
    private static final Logger logger = Exec.getLogger(ParquetHadoopInputPlugin.class);

    public interface PluginTask
            extends Task, ConfigurationFactory.Task
    {
        @Config("path")
        String getPath();

        @Config("parquet_log_level")
        @ConfigDefault("\"INFO\"")
        String getParquetLogLevel();

        List<String> getFiles();
        void setFiles(List<String> files);
    }

    Schema newSchema()
    {
        return Schema.builder().add("record", Types.JSON).build();
    }

    @Override
    public ConfigDiff transaction(ConfigSource config,
            InputPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);
        configureParquetLogger(task);

        Path rootPath = new Path(task.getPath());

        try (PluginClassLoaderScope ignored = new PluginClassLoaderScope()) {
            Configuration conf = ConfigurationFactory.create(task);

            FileSystem fs = FileSystem.get(rootPath.toUri(), conf);
            List<FileStatus> statusList = listFileStatuses(fs, rootPath);
            if (statusList.isEmpty()) {
                throw new PathNotFoundException(rootPath.toString());
            }

            for (FileStatus status : statusList) {
                logger.debug("embulk-input-parquet_hadoop: Loading paths: {}, length: {}",
                        status.getPath(), status.getLen());
            }

            List<String> files = Lists.transform(statusList, new Function<FileStatus, String>() {
                    @Nullable
                    @Override
                    public String apply(@Nullable FileStatus input) {
                        return input.getPath().toString();
                    }
            });
            task.setFiles(files);
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }

        Schema schema = newSchema();
        int taskCount = task.getFiles().size();

        return resume(task.dump(), schema, taskCount, control);
    }

    @Override
    public ConfigDiff resume(TaskSource taskSource,
            Schema schema, int taskCount,
            InputPlugin.Control control)
    {
        control.run(taskSource, schema, taskCount);
        return Exec.newConfigDiff();
    }

    @Override
    public void cleanup(TaskSource taskSource,
            Schema schema, int taskCount,
            List<TaskReport> successTaskReports)
    {
    }

    @Override
    public TaskReport run(TaskSource taskSource,
            Schema schema, int taskIndex,
            PageOutput output)
    {
        PluginTask task = taskSource.loadTask(PluginTask.class);
        configureParquetLogger(task);

        final Column jsonColumn = schema.getColumn(0);

        Configuration conf;
        Path filePath;
        try (PluginClassLoaderScope ignored = new PluginClassLoaderScope()) {
            conf = ConfigurationFactory.create(task);
            filePath = new Path(task.getFiles().get(taskIndex));
        }

        try (PageBuilder pageBuilder = newPageBuilder(schema, output)) {
            ParquetRowReader<Value> reader;
            try (PluginClassLoaderScope ignored = new PluginClassLoaderScope()) {
                reader = new ParquetRowReader<>(conf, filePath, new MessagePackReadSupport());
            } catch (ParquetRuntimeException | IOException e) {
                throw new DataException(e);
            }

            Value value;
            while (true) {
                try (PluginClassLoaderScope ignored = new PluginClassLoaderScope()) {
                    value = reader.read();
                } catch (ParquetRuntimeException | IOException e) {
                    throw new DataException(e);
                }
                if (value == null) {
                    break;
                }

                pageBuilder.setJson(jsonColumn, value);
                pageBuilder.addRecord();
            }

            pageBuilder.finish();

            try (PluginClassLoaderScope ignored = new PluginClassLoaderScope()) {
                reader.close();
            } catch (ParquetRuntimeException | IOException e) {
                throw new DataException(e);
            }
        }

        TaskReport report = Exec.newTaskReport();
        return report;
    }

    @Override
    public ConfigDiff guess(ConfigSource config)
    {
        return Exec.newConfigDiff();
    }

    private PageBuilder newPageBuilder(Schema schema, PageOutput output)
    {
        return new PageBuilder(Exec.getBufferAllocator(), schema, output);
    }

    private List<FileStatus> listFileStatuses(FileSystem fs, Path rootPath) throws IOException {
        List<FileStatus> fileStatuses = Lists.newArrayList();

        FileStatus[] entries = fs.globStatus(rootPath, HiddenFileFilter.INSTANCE);
        if (entries == null) {
            return fileStatuses;
        }

        for (FileStatus entry : entries) {
            if (entry.isDirectory()) {
                List<FileStatus> subEntries = listRecursive(fs, entry);
                fileStatuses.addAll(subEntries);
            } else {
                fileStatuses.add(entry);
            }
        }

        return fileStatuses;
    }

    private List<FileStatus> listRecursive(FileSystem fs, FileStatus status) throws IOException
    {
        List<FileStatus> statusList = Lists.newArrayList();
        if (status.isDirectory()) {
            FileStatus[] entries = fs.listStatus(status.getPath(), HiddenFileFilter.INSTANCE);
            for (FileStatus entry : entries) {
                statusList.addAll(listRecursive(fs, entry));
            }
        } else {
            statusList.add(status);
        }
        return statusList;
    }

    private static void configureParquetLogger(PluginTask task)
    {
        // delegate java.util.logging to slf4j.
        java.util.logging.Logger parquetLogger = java.util.logging.Logger.getLogger("org.apache.parquet");
        if (parquetLogger.getHandlers().length == 0) {
            parquetLogger.addHandler(new SLF4JBridgeHandler());
            parquetLogger.setUseParentHandlers(false);
        }

        Level level;
        try {
            level = Level.parse(task.getParquetLogLevel());
        } catch (IllegalArgumentException e) {
            logger.warn("embulk-input-parquet_hadoop: Invalid parquet_log_level", e);
            level = Level.WARNING;
        }
        // invoke static initializer that overrides log level.
        try {
            Class.forName("org.apache.parquet.Log");
        } catch (ClassNotFoundException e) {
            logger.warn("", e);
        }

        parquetLogger.setLevel(level);
    }
}
