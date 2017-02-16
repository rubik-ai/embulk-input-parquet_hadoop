package studio.adtech.parquet.msgpack.read.converter;

import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.msgpack.value.ValueFactory;

/**
 * Parquet converter for Parquet primitive types.
 */
class ParquetPrimitiveConverter extends PrimitiveConverter implements HasParentContainerUpdater {
    protected final ParentContainerUpdater updater;

    ParquetPrimitiveConverter(ParentContainerUpdater updater) {
        this.updater = updater;
    }

    @Override
    public ParentContainerUpdater getUpdater() {
        return updater;
    }

    @Override
    public void addBoolean(boolean value) {
        getUpdater().setBoolean(value);
    }

    @Override
    public void addInt(int value) {
        getUpdater().setInt(value);
    }

    @Override
    public void addLong(long value) {
        getUpdater().setLong(value);
    }

    @Override
    public void addFloat(float value) {
        getUpdater().setFloat(value);
    }

    @Override
    public void addDouble(double value) {
        getUpdater().setDouble(value);
    }

    @Override
    public void addBinary(Binary value) {
        // It's safe to omit copy because values#getBytes returns copied array.
        getUpdater().set(ValueFactory.newBinary(value.getBytes(), true));
    }
}
