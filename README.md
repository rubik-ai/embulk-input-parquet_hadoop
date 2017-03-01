# Parquet Hadoop input plugin for Embulk

TODO: Write short description here and build.gradle file.

## Overview

* **Plugin type**: input
* **Resume supported**: no
* **Cleanup supported**: yes
* **Guess supported**: no

## Configuration

- **option1**: description (integer, required)
- **option2**: description (string, default: `"myvalue"`)
- **option3**: description (string, default: `null`)

## Hadoop Configuration

- `parquet.read.bad.record.threshold` (float, default: `0`)

## Example

```yaml
in:
  type: parquet_hadoop
  option1: example1
  option2: example2
```


## Build

```
$ ./gradlew gem  # -t to watch change of files and rebuild continuously
```
