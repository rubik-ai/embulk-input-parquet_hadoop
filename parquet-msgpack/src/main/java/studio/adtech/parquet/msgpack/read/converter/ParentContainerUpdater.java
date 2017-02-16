package studio.adtech.parquet.msgpack.read.converter;

import org.msgpack.value.Value;
import org.msgpack.value.ValueFactory;

/**
 * A [[ParentContainerUpdater]] is used by a Parquet converter to set converted values to some
 * corresponding parent container. For example, a converter for a `map` field may set
 * converted values to a [[{@link org.msgpack.value.ImmutableMapValue}]]; or a converter for array
 * elements may append converted values to an [[ArrayList]].
 */
interface ParentContainerUpdater {
    /** Called before a record field is being converted */
    void start();

    /** Called after a record field is being converted */
    void end();

    void set(Value value);

    void setBoolean(boolean value);

    void setByte(byte value);

    void setShort(short value);

    void setInt(int value);

    void setLong(long value);

    void setFloat(float value);

    void setDouble(double value);

    /**
     * A no-op updater used for root converter (who doesn't have a parent).
     */
    class Noop implements ParentContainerUpdater {
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
