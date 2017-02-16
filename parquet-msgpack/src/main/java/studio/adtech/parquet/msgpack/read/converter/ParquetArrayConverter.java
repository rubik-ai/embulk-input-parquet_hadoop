package studio.adtech.parquet.msgpack.read.converter;

import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.schema.GroupType;
import org.msgpack.value.Value;
import org.msgpack.value.ValueFactory;

import java.util.ArrayList;

/**
 * @author Koji Agawa
 */
abstract class ParquetArrayConverter extends ParquetValueConverter {
    protected ArrayList<Value> currentArray;

    private ParquetArrayConverter(GroupType schema, ParentContainerUpdater updater) {
        super(schema, updater);
    }

    @Override
    public void start() {
        currentArray = new ArrayList<>();
    }

    @Override
    public void end() {
        updater.set(ValueFactory.newArray(currentArray));
    }

    static class A extends ParquetArrayConverter {
        private final Converter elementConverter;

        public A(GroupType schema, ParentContainerUpdater updater) {
            super(schema, updater);
            elementConverter = newConverter(schema, new ParentContainerUpdater.Noop() {
                @Override
                public void set(Value value) {
                    currentArray.add(value);
                }
            });
        }

        @Override
        public Converter getConverter(int fieldIndex) {
            return elementConverter;
        }
    }

    static class B extends ParquetArrayConverter {
        private final Converter elementConverter;

        public B(final GroupType schema, ParentContainerUpdater updater) {
            super(schema, updater);

            elementConverter = new GroupConverter() {
                private Value currentElement = ValueFactory.newNil();
                private Converter converter = newConverter(schema, new ParentContainerUpdater.Noop() {
                    @Override
                    public void set(Value value) {
                        currentElement = value;
                    }
                });

                @Override
                public Converter getConverter(int fieldIndex) {
                    return converter;
                }

                @Override
                public void start() {
                    currentElement = ValueFactory.newNil();
                }

                @Override
                public void end() {
                    currentArray.add(currentElement);
                }
            };
        }

        @Override
        public Converter getConverter(int fieldIndex) {
            return elementConverter;
        }

    }
}
