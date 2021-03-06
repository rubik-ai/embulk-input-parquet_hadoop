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
package jp.co.cyberagent.parquet.msgpack.read.converter;

import org.apache.parquet.column.Dictionary;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.msgpack.value.Value;
import org.msgpack.value.ValueFactory;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

/**
 * A [[ParquetValueConverter]] is used to convert Parquet records into Message Pack [[Value]].
 */
public class ParquetValueConverter extends ParquetGroupConverter
{
    // Converters for each field.
    private final Converter[] fieldConverters;

    private InternalMap currentMap;

    // TODO: make configurable
    private boolean assumeBinaryIsString = false;
    private boolean assumeInt96IsTimestamp = true;

    public ParquetValueConverter(GroupType schema, ParentContainerUpdater updater)
    {
        super(updater);

        ArrayList<String> fieldNames = new ArrayList<>();
        for (Type type : schema.getFields()) {
            fieldNames.add(type.getName());
        }
        this.currentMap = new InternalMap(fieldNames);

        this.fieldConverters = new Converter[schema.getFieldCount()];
        int i = 0;
        for (Type field : schema.getFields()) {
            InternalMapUpdater update = new InternalMapUpdater(currentMap, i);
            fieldConverters[i++] = newFieldConverter(field, update);
        }
    }

    Value getCurrentRecord()
    {
        return currentMap.build();
    }

    @Override
    public Converter getConverter(int fieldIndex)
    {
        return fieldConverters[fieldIndex];
    }

    @Override
    public void start()
    {
        int i = 0;
        for (Converter converter : fieldConverters) {
            ((HasParentContainerUpdater) converter).getUpdater().start();
            currentMap.set(i, ValueFactory.newNil());
            i += 1;
        }
    }

    @Override
    public void end()
    {
        for (Converter converter : fieldConverters) {
            ((HasParentContainerUpdater) converter).getUpdater().end();
        }
        getUpdater().set(currentMap.build());
    }

    @Override
    public ParentContainerUpdater getUpdater()
    {
        return updater;
    }

    private Converter newFieldConverter(Type parquetType, ParentContainerUpdater updater)
    {
        if (parquetType.isRepetition(Type.Repetition.REPEATED) && parquetType.getOriginalType() != OriginalType.LIST) {
            // A repeated field that is neither contained by a `LIST`- or `MAP`-annotated group nor
            // annotated by `LIST` or `MAP` should be interpreted as a required list of required
            // elements where the element type is the type of the field.
            if (parquetType.isPrimitive()) {
                return new RepeatedPrimitiveConverter(parquetType, updater);
            }
            else {
                return new RepeatedGroupConverter(parquetType, updater);
            }
        }
        else {
            return newConverter(parquetType, updater);
        }
    }

    private Converter newConverter(Type parquetType, ParentContainerUpdater updater)
    {
        if (parquetType.isPrimitive()) {
            return newConverterForPrimitiveField(parquetType.asPrimitiveType(), updater);
        }
        else {
            return newConverterForGroupField(parquetType.asGroupType(), updater);
        }
    }

    private Converter newConverterForPrimitiveField(PrimitiveType field, final ParentContainerUpdater updater)
    {
        PrimitiveType.PrimitiveTypeName typeName = field.getPrimitiveTypeName();
        OriginalType originalType = field.getOriginalType();
        String typeString = (originalType == null ? String.valueOf(typeName) : String.format("%s (%s)", typeName, originalType));

        switch (typeName) {
            case BOOLEAN:
            case FLOAT:
            case DOUBLE:
                return new ParquetPrimitiveConverter(updater);

            case INT32:
                if (originalType == null) {
                    return new ParquetPrimitiveConverter(updater);
                }
                else {
                    switch (originalType) {
                        case INT_8:
                            return new ParquetPrimitiveConverter(updater) {
                                @Override
                                public void addInt(int value)
                                {
                                    this.getUpdater().setByte((byte) value);
                                }
                            };
                        case INT_16:
                            return new ParquetPrimitiveConverter(updater) {
                                @Override
                                public void addInt(int value)
                                {
                                    this.getUpdater().setShort((short) value);
                                }
                            };
                        case INT_32:
                            return new ParquetPrimitiveConverter(updater);
                        case DATE:
                            return new ParquetPrimitiveConverter(updater) {
                                @Override
                                public void addInt(int value)
                                {
                                    getUpdater().setInt(value);
                                }
                            };
                        case DECIMAL:
                            DecimalType decimal = DecimalType.create(field, DecimalType.MAX_INT_DIGITS);
                            return new ParquetDecimalConverter.IntDictionaryAware(decimal.getPrecision(), decimal.getScale(), updater);
                        case UINT_8:
                        case UINT_16:
                        case UINT_32:
                            throw new ParquetSchemaException("Parquet type not supported: " + typeString);
                        case TIME_MILLIS:
                            throw new ParquetSchemaException("Parquet type not yet supported: " + typeString);
                        default:
                            throw new ParquetSchemaException("Illegal Parquet type: " + typeString);
                    }
                }

            case INT64:
                if (originalType == null) {
                    return new ParquetPrimitiveConverter(updater);
                }
                else {
                    switch (originalType) {
                        case INT_64:
                            return new ParquetPrimitiveConverter(updater);
                        case DECIMAL:
                            DecimalType decimal = DecimalType.create(field, DecimalType.MAX_LONG_DIGITS);
                            return new ParquetDecimalConverter.LongDictionaryAware(decimal.getPrecision(), decimal.getScale(), updater);
                        case UINT_64:
                            throw new ParquetSchemaException("Parquet type not supported: " + typeString);
                        case TIMESTAMP_MILLIS:
                            throw new ParquetSchemaException("Parquet type not yet supported: " + typeString);
                        default:
                            throw new ParquetSchemaException("Illegal Parquet type: " + typeString);
                    }
                }

            case INT96:
                // TODO: document assumeInt96IsTimestamp
                checkConversionRequirement(
                        assumeInt96IsTimestamp,
                        "INT96 is not supported unless it's interpreted as timestamp. " +
                                "Please try to set assumeInt96IsTimestamp to true.");
                return new ParquetPrimitiveConverter(updater) {
                    @Override
                    public void addBinary(Binary value)
                    {
                        if (value.length() != 12) {
                            throw new AssertionError(
                                    "Timestamps (with nanoseconds) are expected to be stored in 12-byte long binaries, " +
                                    "but got a " + String.valueOf(value.length()) + "-byte binary.");
                        }

                        ByteBuffer buf = value.toByteBuffer().order(ByteOrder.LITTLE_ENDIAN);
                        long timeOfDayNanos = buf.getLong();
                        int julianDays = buf.getInt();

                        getUpdater().setLong(DateTimeUtils.fromJulianDay(julianDays, timeOfDayNanos));
                    }
                };

            case BINARY:
                if (originalType == null) {
                    if (assumeBinaryIsString) {
                        return new ParquetStringConverter(updater);
                    }
                    else {
                        return new ParquetPrimitiveConverter(updater);
                    }
                }
                else {
                    switch (originalType) {
                        case UTF8:
                        case ENUM:
                        case JSON:
                            return new ParquetStringConverter(updater);
                        case BSON:
                            return new ParquetPrimitiveConverter(updater);
                        case DECIMAL:
                            DecimalType decimal = DecimalType.create(field);
                            return new ParquetDecimalConverter.BinaryDictionaryAware(decimal.getPrecision(), decimal.getScale(), updater);
                        default:
                            throw new ParquetSchemaException("Illegal Parquet type: " + typeString);
                    }
                }

            case FIXED_LEN_BYTE_ARRAY:
                if (originalType == null) {
                    throw new ParquetSchemaException("Illegal Parquet type: " + typeString);
                }
                else {
                    switch (originalType) {
                        case DECIMAL:
                            DecimalType decimal = DecimalType.create(field);
                            return new ParquetDecimalConverter.BinaryDictionaryAware(decimal.getPrecision(), decimal.getScale(), updater);
                        case INTERVAL:
                            throw new ParquetSchemaException("Parquet type not yet supported: " + typeString);
                        default:
                            throw new ParquetSchemaException("Illegal Parquet type: " + typeString);
                    }
                }

            default:
                throw new ParquetSchemaException("Illegal Parquet type: " + typeString);
        }
    }

    private Converter newConverterForGroupField(GroupType field, final ParentContainerUpdater updater)
    {
        OriginalType originalType = field.getOriginalType();
        if (originalType == null) {
            return new ParquetValueConverter(field, new ParentContainerUpdater.Noop() {
                @Override
                public void set(Value value)
                {
                    updater.set(value);
                }
            });
        }
        else {
            switch (originalType) {
                // A Parquet list is represented as a 3-level structure:
                //
                //   <list-repetition> group <name> (LIST) {
                //     repeated group list {
                //       <element-repetition> <element-type> element;
                //     }
                //   }
                //
                // However, according to the most recent Parquet format spec (not released yet up until
                // writing), some 2-level structures are also recognized for backwards-compatibility.  Thus,
                // we need to check whether the 2nd level or the 3rd level refers to list element type.
                //
                // See: https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#lists
                case LIST:
                    checkConversionRequirement(field.getFieldCount() == 1,
                            "Invalid list type %s", field);

                    Type repeatedType = field.getType(0);
                    checkConversionRequirement(repeatedType.isRepetition(Type.Repetition.REPEATED),
                            "Invalid list type %s", field);

                    return new ParquetArrayConverter(field, updater);

                case MAP:
                case MAP_KEY_VALUE:
                    checkConversionRequirement(
                            field.getFieldCount() == 1 && !field.getType(0).isPrimitive(),
                            "Invalid map type: %s", field);

                    GroupType keyValueType = field.getType(0).asGroupType();
                    checkConversionRequirement(
                            keyValueType.isRepetition(Type.Repetition.REPEATED) && keyValueType.getFieldCount() == 2,
                            "Invalid map type: %s", field);

                    Type keyType = keyValueType.getType(0);
                    checkConversionRequirement(
                            keyType.isPrimitive(),
                            "Map key type is expected to be a primitive type, but found: %s", keyType);

                    Type valueType = keyValueType.getType(1);

                    return new ParquetMapConverter(updater, keyType, valueType);

                default:
                    throw new ParquetSchemaException("Unrecognized Parquet type: " + field);
            }
        }
    }

    private static boolean isElementType(Type repeatedType, String parentName)
    {
        return (
                // For legacy 2-level list types with primitive element type, e.g.:
                //
                //    // ARRAY<INT> (nullable list, non-null elements)
                //    optional group my_list (LIST) {
                //      repeated int32 element;
                //    }
                //
                repeatedType.isPrimitive()
        ) || (
                // For legacy 2-level list types whose element type is a group type with 2 or more fields,
                // e.g.:
                //
                //    // ARRAY<STRUCT<str: STRING, num: INT>> (nullable list, non-null elements)
                //    optional group my_list (LIST) {
                //      repeated group element {
                //        required binary str (UTF8);
                //        required int32 num;
                //      };
                //    }
                //
                repeatedType.asGroupType().getFieldCount() > 1
        ) || (
                // For legacy 2-level list types generated by parquet-avro (Parquet version < 1.6.0), e.g.:
                //
                //    // ARRAY<STRUCT<str: STRING>> (nullable list, non-null elements)
                //    optional group my_list (LIST) {
                //      repeated group array {
                //        required binary str (UTF8);
                //      };
                //    }
                //
                "array".equals(repeatedType.getName())
        ) || (
                // For Parquet data generated by parquet-thrift, e.g.:
                //
                //    // ARRAY<STRUCT<str: STRING>> (nullable list, non-null elements)
                //    optional group my_list (LIST) {
                //      repeated group my_list_tuple {
                //        required binary str (UTF8);
                //      };
                //    }
                //
                (parentName + "_tuple").equals(repeatedType.getName())
        );
    }

    private static void checkConversionRequirement(boolean condition, String message, Object... args)
    {
        if (!condition) {
            throw new ParquetSchemaException(String.format(message, args));
        }
    }

    /**
     * Mutable fixed-length map.
     */
    private static class InternalMap
    {
        private final int numFields;
        private final Value[] kvs;

        public InternalMap(List<String> keys)
        {
            this.numFields = keys.size();
            this.kvs = new Value[numFields * 2];
            int i = 0;
            for (String key : keys) {
                kvs[i++] = ValueFactory.newString(key);
                kvs[i++] = ValueFactory.newNil();
            }
        }

        public void set(int index, Value value)
        {
            kvs[index * 2 + 1] = value;
        }

        public Value build()
        {
            return ValueFactory.newMap(kvs, false);
        }
    }

    /**
     * Updater used together with field converters within a [[ParquetValueConverter]].  It propagates
     * converted filed values to the `index`-th cell in `currentMap`.
     */
    private static final class InternalMapUpdater extends ParentContainerUpdater.Noop
    {
        private final InternalMap map;
        private final int index;

        InternalMapUpdater(InternalMap map, int index)
        {
            this.map = map;
            this.index = index;
        }

        @Override
        public void set(Value value)
        {
            map.set(index, value);
        }

        @Override
        public void setBoolean(boolean value)
        {
            map.set(index, ValueFactory.newBoolean(value));
        }

        @Override
        public void setByte(byte value)
        {
            map.set(index, ValueFactory.newInteger(value));
        }

        @Override
        public void setShort(short value)
        {
            map.set(index, ValueFactory.newInteger(value));
        }

        @Override
        public void setInt(int value)
        {
            map.set(index, ValueFactory.newInteger(value));
        }

        @Override
        public void setLong(long value)
        {
            map.set(index, ValueFactory.newInteger(value));
        }

        @Override
        public void setFloat(float value)
        {
            map.set(index, ValueFactory.newFloat(value));
        }

        @Override
        public void setDouble(double value)
        {
            map.set(index, ValueFactory.newFloat(value));
        }
    }

    /**
     * Parquet converter for arrays.  Spark SQL arrays are represented as Parquet lists.  Standard
     * Parquet lists are represented as a 3-level group annotated by `LIST`:
     * {{{
     *   <list-repetition> group <name> (LIST) {            <-- parquetSchema points here
     *     repeated group list {
     *       <element-repetition> <element-type> element;
     *     }
     *   }
     * }}}
     * The `parquetSchema` constructor argument points to the outermost group.
     *
     * However, before this representation is standardized, some Parquet libraries/tools also use some
     * non-standard formats to represent list-like structures.  Backwards-compatibility rules for
     * handling these cases are described in Parquet format spec.
     *
     * @see https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#lists
     */
    private class ParquetArrayConverter extends ParquetGroupConverter
    {
        private ArrayList<Value> currentArray;
        private Converter elementConverter;

        private ParquetArrayConverter(GroupType schema, ParentContainerUpdater updater)
        {
            super(updater);

            Type repeatedType = schema.getType(0);
            if (isElementType(repeatedType, schema.getName())) {
                // If the repeated field corresponds to the element type, creates a new converter using the
                // type of the repeated field.
                elementConverter = newConverter(repeatedType, new ParentContainerUpdater.Noop() {
                    @Override
                    public void set(Value value)
                    {
                        ParquetArrayConverter.this.currentArray.add(value);
                    }
                });
            }
            else {
                // If the repeated field corresponds to the syntactic group in the standard 3-level Parquet
                // LIST layout, creates a new converter using the only child field of the repeated field.
                elementConverter = new ElementConverter(repeatedType.asGroupType().getType(0));
            }
        }

        @Override
        public Converter getConverter(int fieldIndex)
        {
            return elementConverter;
        }

        // NOTE: We can't reuse the mutable `ArrayList` here and must instantiate a new buffer for the
        // next value.
        @Override
        public void start()
        {
            currentArray = new ArrayList<>();
        }

        @Override
        public void end()
        {
            getUpdater().set(ValueFactory.newArray(currentArray));
        }

        /** Array element converter */
        private class ElementConverter extends GroupConverter
        {
            private Value currentElement;
            private Converter converter;

            public ElementConverter(Type parquetType)
            {
                converter = newConverter(parquetType, new ParentContainerUpdater.Noop() {
                    @Override
                    public void set(Value value)
                    {
                        currentElement = value;
                    }
                });
            }

            @Override
            public Converter getConverter(int fieldIndex)
            {
                return converter;
            }

            @Override
            public void start()
            {
                currentElement = ValueFactory.newNil();
            }

            @Override
            public void end()
            {
                ParquetArrayConverter.this.currentArray.add(currentElement);
            }
        }
    }

    /** Parquet converter for maps */
    private class ParquetMapConverter extends ParquetGroupConverter
    {
        private final KeyValueConverter keyValueConverter;

        private ArrayList<Value> kvs;

        ParquetMapConverter(ParentContainerUpdater updater, Type keyType, Type valueType)
        {
            super(updater);
            keyValueConverter = new KeyValueConverter(keyType, valueType);
        }

        @Override
        public Converter getConverter(int fieldIndex)
        {
            return keyValueConverter;
        }

        @Override
        public void start()
        {
            kvs = new ArrayList<>();
        }

        @Override
        public void end()
        {
            Value mapValue = ValueFactory.newMap(kvs.toArray(new Value[kvs.size()]));
            getUpdater().set(mapValue);
        }

        private final class KeyValueConverter extends GroupConverter
        {
            private final Converter[] converters;

            private Value currentKey = ValueFactory.newNil();
            private Value currentValue = ValueFactory.newNil();

            KeyValueConverter(Type keyType, Type valueType)
            {
                this.converters = new Converter[] {
                        // Converter for keys
                        newConverter(keyType, new ParentContainerUpdater.Noop() {
                            @Override
                            public void set(Value value)
                            {
                                currentKey = value;
                            }
                        }),
                        // Converter for values
                        newConverter(valueType, new ParentContainerUpdater.Noop() {
                            @Override
                            public void set(Value value)
                            {
                                currentValue = value;
                            }
                        })
                };
            }

            @Override
            public Converter getConverter(int fieldIndex)
            {
                return converters[fieldIndex];
            }

            @Override
            public void start()
            {
                currentKey = ValueFactory.newNil();
                currentValue = ValueFactory.newNil();
            }

            @Override
            public void end()
            {
                ParquetMapConverter.this.kvs.add(currentKey);
                ParquetMapConverter.this.kvs.add(currentValue);
            }
        }
    }

    /**
     * A primitive converter for converting unannotated repeated primitive values to required arrays
     * of required primitives values.
     */
    private class RepeatedPrimitiveConverter extends PrimitiveConverter implements HasParentContainerUpdater
    {
        private final ParentContainerUpdater updater;
        private final PrimitiveConverter elementConverter;
        private ArrayList<Value> currentArray;

        public RepeatedPrimitiveConverter(Type parquetType, final ParentContainerUpdater parentUpdater)
        {
            this.updater = new ParentContainerUpdater.Noop() {
                @Override
                public void start()
                {
                    RepeatedPrimitiveConverter.this.currentArray = new ArrayList<>();
                }

                @Override
                public void end()
                {
                    parentUpdater.set(ValueFactory.newArray(RepeatedPrimitiveConverter.this.currentArray));
                }

                @Override
                public void set(Value value)
                {
                    RepeatedPrimitiveConverter.this.currentArray.add(value);
                }
            };

            this.elementConverter = newConverter(parquetType, getUpdater()).asPrimitiveConverter();
        }

        @Override
        public ParentContainerUpdater getUpdater()
        {
            return updater;
        }

        @Override
        public boolean hasDictionarySupport()
        {
            return elementConverter.hasDictionarySupport();
        }

        @Override
        public void setDictionary(Dictionary dictionary)
        {
            elementConverter.setDictionary(dictionary);
        }

        @Override
        public void addValueFromDictionary(int dictionaryId)
        {
            elementConverter.addValueFromDictionary(dictionaryId);
        }

        @Override
        public void addBinary(Binary value)
        {
            elementConverter.addBinary(value);
        }

        @Override
        public void addBoolean(boolean value)
        {
            elementConverter.addBoolean(value);
        }

        @Override
        public void addDouble(double value)
        {
            elementConverter.addDouble(value);
        }

        @Override
        public void addFloat(float value)
        {
            elementConverter.addFloat(value);
        }

        @Override
        public void addInt(int value)
        {
            elementConverter.addInt(value);
        }

        @Override
        public void addLong(long value)
        {
            elementConverter.addLong(value);
        }
    }

    /**
     * A group converter for converting unannotated repeated group values to required arrays of
     * required struct values.
     */
    private class RepeatedGroupConverter extends GroupConverter implements HasParentContainerUpdater
    {
        private final ParentContainerUpdater updater;
        private final GroupConverter elementConverter;
        private ArrayList<Value> currentArray;

        public RepeatedGroupConverter(Type parquetType, final ParentContainerUpdater parentUpdater)
        {
            this.updater = new ParentContainerUpdater.Noop() {
                @Override
                public void start()
                {
                    RepeatedGroupConverter.this.currentArray = new ArrayList<>();
                }

                @Override
                public void end()
                {
                    parentUpdater.set(ValueFactory.newArray(RepeatedGroupConverter.this.currentArray));
                }

                @Override
                public void set(Value value)
                {
                    RepeatedGroupConverter.this.currentArray.add(value);
                }
            };

            this.elementConverter = newConverter(parquetType, getUpdater()).asGroupConverter();
        }

        @Override
        public ParentContainerUpdater getUpdater()
        {
            return updater;
        }

        @Override
        public Converter getConverter(int fieldIndex)
        {
            return elementConverter.getConverter(fieldIndex);
        }

        @Override
        public void start()
        {
            elementConverter.start();
        }

        @Override
        public void end()
        {
            elementConverter.end();
        }
    }
}
