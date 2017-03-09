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

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;

import java.io.File;
import java.io.IOException;

public class JSONLinesIterator implements JSONIterator
{
    private final LineIterator inner;

    public static JSONLinesIterator fromResource(String name)
    {
        File file = new File(Thread.currentThread().getContextClassLoader().getResource(name).getPath());
        return new JSONLinesIterator(file);
    }

    public JSONLinesIterator(File file)
    {
        try {
            inner = FileUtils.lineIterator(file);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean hasNext()
    {
        return inner.hasNext();
    }

    @Override
    public String next()
    {
        return inner.next();
    }

    @Override
    public void remove()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws IOException
    {
        inner.close();
    }
}
