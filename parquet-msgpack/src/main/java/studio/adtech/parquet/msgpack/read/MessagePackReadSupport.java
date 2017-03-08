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
package studio.adtech.parquet.msgpack.read;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;
import org.msgpack.value.Value;
import studio.adtech.parquet.msgpack.read.converter.MessagePackRecordMaterializer;

import java.util.Map;

public class MessagePackReadSupport extends ReadSupport<Value>
{
    @Override
    public RecordMaterializer<Value> prepareForRead(Configuration configuration, Map<String, String> keyValueMetaData, MessageType fileSchema, ReadContext readContext)
    {
        return new MessagePackRecordMaterializer(fileSchema);
    }

    @Override
    public ReadContext init(InitContext context)
    {
        return new ReadContext(context.getFileSchema());
    }
}
