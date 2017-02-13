package org.embulk.input.parquet_hadoop.read.converter;

import org.msgpack.value.Value;
import org.msgpack.value.ValueFactory;

/**
 * @author Koji Agawa
 */
interface ParentContainerUpdater {
    void start();

    void end();

    void set(Value value);

    void setBoolean(boolean value);

    void setByte(byte value);

    void setShort(short value);

    void setInt(int value);

    void setLong(long value);

    void setFloat(float value);

    void setDouble(double value);

    abstract class Default implements ParentContainerUpdater {
        @Override
        public void start() {
        }

        @Override
        public void end() {
        }

        @Override
        public void set(Value value) {
        }

        @Override
        public void setBoolean(boolean value) {
            set(ValueFactory.newBoolean(value));
        }

        @Override
        public void setByte(byte value) {
            set(ValueFactory.newInteger(value));
        }

        @Override
        public void setShort(short value) {
            set(ValueFactory.newInteger(value));
        }

        @Override
        public void setInt(int value) {
            set(ValueFactory.newInteger(value));
        }

        @Override
        public void setLong(long value) {
            set(ValueFactory.newInteger(value));
        }

        @Override
        public void setFloat(float value) {
            set(ValueFactory.newFloat(value));
        }

        @Override
        public void setDouble(double value) {
            set(ValueFactory.newFloat(value));
        }
    }
}
