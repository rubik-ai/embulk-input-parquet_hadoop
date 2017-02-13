package studio.adtech.parquet.msgpack.read.converter;

import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;
import studio.adtech.parquet.msgpack.read.converter.MessagePackRecordConverter;
import org.msgpack.value.Value;
import studio.adtech.parquet.msgpack.read.converter.ParentContainerUpdater;

/**
 * @author Koji Agawa
 */
public class MessagePackRecordMaterializer extends RecordMaterializer<Value> {
    private final MessagePackRecordConverter root;

    public MessagePackRecordMaterializer(MessageType schema) {
        this.root = new MessagePackRecordConverter(schema, new ParentContainerUpdater.Default() {
        });
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
