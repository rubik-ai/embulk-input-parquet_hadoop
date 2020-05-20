# Parquet Hadoop input plugin for Embulk

- Read Parquet files via Hadoop FileSystem.
- Outputs a single record named `record` (type is `json`).

## Overview

* **Plugin type**: input
* **Resume supported**: no
* **Cleanup supported**: no
* **Guess supported**: no

## Configuration

- **`config_files`**: list of paths to Hadoop's configuration files (array of strings, default: `[]`)
- **`config`**: overwrites configuration parameters (hash, default: `{}`)
- **`path`**: file path on Hdfs. you can use glob pattern (string, required).
- **`parquet_log_level`**: set log level of parquet reader module (string, default: `"INFO"`)
   - value is one of `java.util.logging.Level` (ALL, SEVERE, WARNING, INFO, CONFIG, FINE, FINER, FINEST, OFF)

### Hadoop Configuration

Describe parquet reader specific configuration. 

- **`parquet.read.bad.record.threshold`**:
  Tolerated percent bad records per file
  (float, 0.0 to 1.0, default: `0`)


## Example

```yaml
in:
  type: parquet_hadoop
  config_files:
    - /etc/hadoop/conf/core-site.xml
    - /etc/hadoop/conf/hdfs-site.xml
  config:
    parquet.read.bad.record.threshold: 0.01
  path: /user/hadoop/example/data/*.parquet
  parquet_log_level: WARNING
```

## Build

```
$ ./gradlew gem  # -t to watch change of files and rebuild continuously
```

## Install gem
```
$ embulk gem install <pkg/.gemfile>
```

## Notes

### Why implement this as input plugin rather than parser plugin ?

Because to parsing Parquet format needs seekable file stream
but parser plugin has only sequential read.

### How map Parquet schema to json ?

TBD
