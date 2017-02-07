package org.embulk.input.parquet_hadoop.read;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;
import org.msgpack.value.Value;

import java.util.Map;

/**
 * @author Koji Agawa
 */
public class MessagePackReadSupport extends ReadSupport<Value> {
    @Override
    public RecordMaterializer<Value> prepareForRead(Configuration configuration, Map<String, String> keyValueMetaData, MessageType fileSchema, ReadContext readContext) {
        return new MessagePackRecordMaterializer(fileSchema);
    }

    @Override
    public ReadContext init(InitContext context) {
        return new ReadContext(context.getFileSchema());
    }
}
