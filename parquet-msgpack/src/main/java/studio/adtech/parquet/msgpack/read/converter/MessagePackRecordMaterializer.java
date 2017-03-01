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
package studio.adtech.parquet.msgpack.read.converter;

import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;
import org.msgpack.value.Value;

public class MessagePackRecordMaterializer extends RecordMaterializer<Value> {
    private final ParquetValueConverter root;

    public MessagePackRecordMaterializer(MessageType schema) {
        this.root = new ParquetValueConverter(schema, new ParentContainerUpdater.Noop());
    }

    @Override
    public Value getCurrentRecord() {
        return root.getCurrentRecord();
    }

    @Override
    public GroupConverter getRootConverter() {
        return root;
    }
}
