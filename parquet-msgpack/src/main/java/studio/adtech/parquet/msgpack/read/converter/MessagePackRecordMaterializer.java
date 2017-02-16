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
