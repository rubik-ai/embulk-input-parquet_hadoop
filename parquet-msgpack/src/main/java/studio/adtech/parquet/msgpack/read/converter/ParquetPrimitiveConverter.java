package studio.adtech.parquet.msgpack.read.converter;

import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.msgpack.value.ValueFactory;

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
        updater.setBoolean(value);
    }

    @Override
    public void addInt(int value) {
        updater.setInt(value);
    }

    @Override
    public void addLong(long value) {
        updater.setLong(value);
    }

    @Override
    public void addFloat(float value) {
        updater.setFloat(value);
    }

    @Override
    public void addDouble(double value) {
        updater.setDouble(value);
    }

    @Override
    public void addBinary(Binary value) {
        updater.set(ValueFactory.newBinary(value.getBytes()));
    }
}
