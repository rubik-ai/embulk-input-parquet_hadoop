package org.embulk.input.parquet_hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetReader;
import org.embulk.config.Config;
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
import studio.adtech.parquet.msgpack.read.MessagePackReadSupport;

import java.io.IOException;
import java.util.List;

public class ParquetHadoopInputPlugin
        implements InputPlugin
{
    public interface PluginTask
            extends Task, ConfigurationFactory.Task
    {
        @Config("path")
        String getPath();
        // configuration option 1 (required integer)
//        @Config("option1")
//        public int getOption1();

        // configuration option 2 (optional string, null is not allowed)
//        @Config("option2")
//        @ConfigDefault("\"myvalue\"")
//        public String getOption2();

        // configuration option 3 (optional string, null is allowed)
//        @Config("option3")
//        @ConfigDefault("null")
//        public Optional<String> getOption3();

        // if you get schema from config
//        @Config("columns")
//        public SchemaConfig getColumns();
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

        Configuration conf = ConfigurationFactory.create(task);

        Schema schema = newSchema();
        int taskCount = 1;  // number of run() method calls

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

        final Column column = schema.getColumn(0);

        try (PageBuilder pageBuilder = newPageBuilder(schema, output)) {
            try {
                Configuration conf = new Configuration();
                conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

                ParquetReader<Value> reader = ParquetReader.builder(new MessagePackReadSupport(), new Path(task.getPath()))
                        .withConf(conf)
                        .build();
                for (Value value = reader.read(); value != null; value = reader.read()) {
                    pageBuilder.setJson(column, value);
                    pageBuilder.addRecord();
                }
                pageBuilder.finish();
                reader.close();
            } catch (IOException e) {
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
}
