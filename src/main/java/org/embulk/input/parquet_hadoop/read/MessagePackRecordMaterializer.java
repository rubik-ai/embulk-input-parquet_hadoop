package org.embulk.input.parquet_hadoop.read;

import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;
import org.embulk.input.parquet_hadoop.read.converter.MessagePackRecordConverter;
import org.msgpack.value.Value;

/**
 * @author Koji Agawa
 */
public class MessagePackRecordMaterializer extends RecordMaterializer<Value> {
    private final MessagePackRecordConverter root;

    public MessagePackRecordMaterializer(MessageType schema) {
        this.root = new MessagePackRecordConverter(schema);
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
