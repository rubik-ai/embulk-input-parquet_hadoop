package studio.adtech.parquet.msgpack.read.converter;

import org.apache.parquet.io.api.GroupConverter;

/**
 * A convenient converter class for Parquet group types with a [[HasParentContainerUpdater]].
 */
public abstract class ParquetGroupConverter extends GroupConverter implements HasParentContainerUpdater {
    protected final ParentContainerUpdater updater;

    public ParquetGroupConverter(ParentContainerUpdater updater) {
        this.updater = updater;
    }

    @Override
    public ParentContainerUpdater getUpdater() {
        return updater;
    }
}
