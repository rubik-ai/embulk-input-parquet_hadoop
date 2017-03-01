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
