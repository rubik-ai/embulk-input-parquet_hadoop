package org.embulk.input.parquet_hadoop.read.converter;

import org.apache.parquet.column.Dictionary;
import org.apache.parquet.io.api.Binary;
import org.msgpack.value.Value;
import org.msgpack.value.ValueFactory;

/**
 * Parquet converter for strings. A dictionary is used to minimize string decoding cost.
 */
class ParquetStringConverter extends ParquetPrimitiveConverter {
    private Value[] expandedDictionary = null;

    ParquetStringConverter(ParentContainerUpdater updater) {
        super(updater);
    }

    @Override
    public boolean hasDictionarySupport() {
        return true;
    }

    @Override
    public void setDictionary(Dictionary dictionary) {
        expandedDictionary = new Value[dictionary.getMaxId() + 1];
        for (int id = 0; id <= dictionary.getMaxId(); id++) {
            // This is copied array. Copying at ValueFactory#newString is not necessary.
            byte[] bytes = dictionary.decodeToBinary(id).getBytes();
            expandedDictionary[id] = ValueFactory.newString(bytes);
        }
    }

    @Override
    public void addValueFromDictionary(int dictionaryId) {
        updater.set(expandedDictionary[dictionaryId]);
    }

    @Override
    public void addBinary(Binary value) {
        // This is copied array. Copying at ValueFactory#newString is not necessary.
        byte[] bytes = value.getBytes();
        updater.set(ValueFactory.newString(bytes));
    }
}
