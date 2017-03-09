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
package jp.co.cyberagent.parquet.msgpack;

import jp.co.cyberagent.parquet.msgpack.read.MessagePackReadSupport;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetReader;
import org.msgpack.value.Value;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;

public class ParquetIterator implements Iterator<Value>, Closeable
{
    private final ParquetReader<Value> reader;
    private Value item;

    public static ParquetIterator fromResource(String name)
    {
        Path path = new Path(Thread.currentThread().getContextClassLoader().getResource(name).getPath());
        return new ParquetIterator(path);
    }

    public ParquetIterator(Path path)
    {
        try {
            reader = ParquetReader.builder(new MessagePackReadSupport(), path).build();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        item = read();
    }

    private Value read()
    {
        try {
            return reader.read();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean hasNext()
    {
        return item != null;
    }

    @Override
    public Value next()
    {
        Value ret = this.item;
        this.item = read();
        return ret;
    }

    @Override
    public void remove()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws IOException
    {
        reader.close();
    }
}
