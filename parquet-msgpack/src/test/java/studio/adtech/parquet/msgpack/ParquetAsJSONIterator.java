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
package studio.adtech.parquet.msgpack;

import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class ParquetAsJSONIterator implements JSONIterator
{
    private final ParquetIterator inner;

    public static ParquetAsJSONIterator fromResource(String name)
    {
        Path path = new Path(Thread.currentThread().getContextClassLoader().getResource(name).getPath());
        return new ParquetAsJSONIterator(path);
    }

    public ParquetAsJSONIterator(Path path)
    {
        inner = new ParquetIterator(path);
    }

    @Override
    public boolean hasNext()
    {
        return inner.hasNext();
    }

    @Override
    public String next()
    {
        return inner.next().toString();
    }

    @Override
    public void remove()
    {
        inner.remove();
    }

    @Override
    public void close() throws IOException
    {
        inner.close();
    }
}
