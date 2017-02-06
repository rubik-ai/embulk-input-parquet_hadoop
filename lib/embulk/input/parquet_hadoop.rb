Embulk::JavaPlugin.register_input(
  "parquet_hadoop", "org.embulk.input.parquet_hadoop.ParquetHadoopInputPlugin",
  File.expand_path('../../../../classpath', __FILE__))
