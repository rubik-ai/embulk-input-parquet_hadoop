package studio.adtech.parquet.msgpack.read.converter;

import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.Type;
import org.msgpack.value.Value;
import org.msgpack.value.ValueFactory;

import java.util.ArrayList;

/**
 * @author Koji Agawa
 */
class ParquetMapConverter extends MessagePackRecordConverter {
    private final KeyValueConverter keyValueConverter;

    private ArrayList<Value> kvs;

    ParquetMapConverter(GroupType schema, ParentContainerUpdater updater, Type keyType, Type valueType) {
        super(schema, updater);
        keyValueConverter = new KeyValueConverter(keyType, valueType);
    }

    @Override
    public Converter getConverter(int fieldIndex) {
        return keyValueConverter;
    }

    @Override
    public void start() {
        kvs = new ArrayList<>();
    }

    @Override
    public void end() {
        updater.set(ValueFactory.newMap(kvs.toArray(new Value[kvs.size()])));
    }

    private final class KeyValueConverter extends GroupConverter {
        private final Converter[] converters;

        private Value currentKey = ValueFactory.newNil();
        private Value currentValue = ValueFactory.newNil();

        KeyValueConverter(Type keyType, Type valueType) {
            this.converters = new Converter[] {
                    // Converter for keys
                    newConverter(keyType, new ParentContainerUpdater.Default() {
                        @Override
                        public void set(Value value) {
                            currentKey = value;
                        }
                    }),
                    // Converter for values
                    newConverter(valueType, new ParentContainerUpdater.Default() {
                        @Override
                        public void set(Value value) {
                            currentValue = value;
                        }
                    })
            };
        }

        @Override
        public Converter getConverter(int fieldIndex) {
            return converters[fieldIndex];
        }

        @Override
        public void start() {
            currentKey = ValueFactory.newNil();
            currentValue = ValueFactory.newNil();
        }

        @Override
        public void end() {
            ParquetMapConverter.this.kvs.add(currentKey);
            ParquetMapConverter.this.kvs.add(currentValue);
        }
    }
}
