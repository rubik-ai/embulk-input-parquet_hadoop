package org.embulk.input.parquet_hadoop.read.converter;

import org.apache.parquet.column.Dictionary;
import org.apache.parquet.io.api.Binary;
import org.msgpack.value.Value;
import org.msgpack.value.ValueFactory;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;

/**
 * Parquet converter for fixed-precision decimals.
 */
abstract class ParquetDecimalConverter extends ParquetPrimitiveConverter {
    private final int precision;
    private final int scale;
    private final MathContext mc;

    protected Value[] expandedDictionary = null;

    ParquetDecimalConverter(int precision, int scale, ParentContainerUpdater updater) {
        super(updater);
        this.precision = precision;
        this.scale = scale;
        this.mc = new MathContext(precision, RoundingMode.UNNECESSARY);
    }

    @Override
    public boolean hasDictionarySupport() {
        return true;
    }

    @Override
    public void addValueFromDictionary(int dictionaryId) {
        updater.set(expandedDictionary[dictionaryId]);
    }

    // Converts decimals stored as INT32
    @Override
    public void addInt(int value) {
        addLong((long)value);
    }

    // Converts decimals stored as INT64
    @Override
    public void addLong(long value) {
        updater.set(decimalFromLong(value));
    }

    // Converts decimals stored as either FIXED_LENGTH_BYTE_ARRAY or BINARY
    @Override
    public void addBinary(Binary value) {
        updater.set(decimalFromBinary(value));
    }

    protected Value decimalFromLong(long value) {
        // TODO: FIXME
        return ValueFactory.newFloat(new BigDecimal(value, mc).movePointLeft(scale).doubleValue());
    }

    protected Value decimalFromBinary(Binary value) {
        // TODO: FIXME
        return ValueFactory.newNil();
    }

    static class IntDictionaryAware extends ParquetDecimalConverter {
        IntDictionaryAware(int precision, int scale, ParentContainerUpdater updater) {
            super(precision, scale, updater);
        }

        @Override
        public void setDictionary(Dictionary dictionary) {
            expandedDictionary = new Value[dictionary.getMaxId() + 1];
            for (int id = 0; id <= dictionary.getMaxId(); id++) {
                expandedDictionary[id] = decimalFromLong(dictionary.decodeToInt(id));
            }
        }
    }

    static class LongDictionaryAware extends ParquetDecimalConverter {
        LongDictionaryAware(int precision, int scale, ParentContainerUpdater updater) {
            super(precision, scale, updater);
        }

        @Override
        public void setDictionary(Dictionary dictionary) {
            expandedDictionary = new Value[dictionary.getMaxId() + 1];
            for (int id = 0; id <= dictionary.getMaxId(); id++) {
                expandedDictionary[id] = decimalFromLong(dictionary.decodeToLong(id));
            }
        }
    }

    static class BinaryDictionaryAware extends ParquetDecimalConverter {
        BinaryDictionaryAware(int precision, int scale, ParentContainerUpdater updater) {
            super(precision, scale, updater);
        }

        @Override
        public void setDictionary(Dictionary dictionary) {
            expandedDictionary = new Value[dictionary.getMaxId() + 1];
            for (int id = 0; id <= dictionary.getMaxId(); id++) {
                expandedDictionary[id] = decimalFromBinary(dictionary.decodeToBinary(id));
            }
        }
    }
}
