/*
 * This class includes code from Apache Spark.
 *
 * Copyright 2017 CyberAgent, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package studio.adtech.parquet.msgpack.read.converter;

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
