package org.embulk.input.parquet_hadoop.read;

import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.Type;
import org.msgpack.value.Value;
import org.msgpack.value.ValueFactory;

/**
 * @author Koji Agawa
 */
public class MessagePackRecordConverter extends GroupConverter {
    private final Converter[] converters;
    private final String name;
    private final MessagePackRecordConverter parent;

    protected ValueFactory.MapBuilder mapBuilder;

    public MessagePackRecordConverter(GroupType schema) {
        this(schema, "__null__", null);
    }

    public MessagePackRecordConverter(GroupType schema, String name, MessagePackRecordConverter parent) {
        this.converters = new Converter[schema.getFieldCount()];
        this.name = name;
        this.parent = parent;

        int i = 0;
        for (Type field : schema.getFields()) {
            converters[i++] = createConverter(field);
        }
    }

    private Converter createConverter(Type field) {
        OriginalType otype = field.getOriginalType();

        if (field.isPrimitive()) {
            if (otype != null) {
                switch (otype) {
                    case MAP: break;
                    case LIST: break;
                    case UTF8: return new StringConverter(field.getName());
                    case MAP_KEY_VALUE: break;
                    case ENUM: break;
                }
            }

            return new SimplePrimitiveConverter(field.getName());
        }

        GroupType groupType = field.asGroupType();
        if (otype != null) {
            switch (otype) {
                //case MAP:
            }
        }

        return new MessagePackRecordConverter(groupType, field.getName(), this);
    }

    Value getCurrentRecord() {
        return mapBuilder.build();
    }

    protected ValueFactory.MapBuilder getCurrentMapBuilder() {
        return mapBuilder;
    }

    @Override
    public Converter getConverter(int fieldIndex) {
        return converters[fieldIndex];
    }

    @Override
    public void start() {
        mapBuilder = ValueFactory.newMapBuilder();
    }

    @Override
    public void end() {
        if (parent != null) {
            parent.getCurrentMapBuilder().put(ValueFactory.newString(name), mapBuilder.build());
        }
    }

    private class SimplePrimitiveConverter extends PrimitiveConverter {
        protected final Value name;

        SimplePrimitiveConverter(String name) {
            this.name = ValueFactory.newString(name);
        }

        @Override
        public void addBinary(Binary value) {
            mapBuilder.put(name, ValueFactory.newBinary(value.getBytes()));
        }

        @Override
        public void addBoolean(boolean value) {
            mapBuilder.put(name, ValueFactory.newBoolean(value));
        }

        @Override
        public void addDouble(double value) {
            mapBuilder.put(name, ValueFactory.newFloat(value));
        }

        @Override
        public void addFloat(float value) {
            mapBuilder.put(name, ValueFactory.newFloat(value));
        }

        @Override
        public void addInt(int value) {
            mapBuilder.put(name, ValueFactory.newInteger(value));
        }

        @Override
        public void addLong(long value) {
            mapBuilder.put(name, ValueFactory.newInteger(value));
        }
    }

    private class StringConverter extends SimplePrimitiveConverter {
        StringConverter(String name) {
            super(name);
        }

        @Override
        public void addBinary(Binary value) {
            mapBuilder.put(name, ValueFactory.newString(value.toStringUsingUTF8()));
        }
    }

}
