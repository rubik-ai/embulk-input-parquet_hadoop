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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.math.RoundingMode;
import java.nio.ByteBuffer;

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
        getUpdater().set(expandedDictionary[dictionaryId]);
    }

    // Converts decimals stored as INT32
    @Override
    public void addInt(int value) {
        addLong((long)value);
    }

    // Converts decimals stored as INT64
    @Override
    public void addLong(long value) {
        getUpdater().set(decimalFromLong(value));
    }

    // Converts decimals stored as either FIXED_LENGTH_BYTE_ARRAY or BINARY
    @Override
    public void addBinary(Binary value) {
        getUpdater().set(decimalFromBinary(value));
    }

    protected Value decimalFromLong(long value) {
        // TODO: support string conversion
        return ValueFactory.newFloat(new BigDecimal(value, mc).movePointLeft(scale).doubleValue());
    }

    protected Value decimalFromBinary(Binary value) {
        BigDecimal decimal;
        if (precision < DecimalType.MAX_LONG_DIGITS) {
            // Constructs a `Decimal` with an unscaled `Long` value if possible.
            long unscaled = binaryToUnscaledLong(value);
            decimal = new BigDecimal(unscaled, mc).movePointLeft(scale);
        } else {
            // Otherwise, resorts to an unscaled `BigInteger` instead.
            decimal = new BigDecimal(new BigInteger(value.getBytes()), scale);
        }
        // TODO: support string conversion
        return ValueFactory.newFloat(decimal.doubleValue());
    }

    private static long binaryToUnscaledLong(Binary binary) {
        // The underlying `ByteBuffer` implementation is guaranteed to be `HeapByteBuffer`, so here
        // we are using `Binary.toByteBuffer.array()` to steal the underlying byte array without
        // copying it.
        ByteBuffer buffer = binary.toByteBuffer();
        byte[] bytes = buffer.array();
        int start = buffer.arrayOffset() + buffer.position();
        int end = buffer.arrayOffset() + buffer.limit();

        long unscaled = 0L;
        int i = start;

        while (i < end) {
            unscaled = (unscaled << 8) | (bytes[i] & 0xff);
            i += 1;
        }

        int bits = 8 * (end - start);
        unscaled = (unscaled << (64 - bits)) >> (64 - bits);
        return unscaled;
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
