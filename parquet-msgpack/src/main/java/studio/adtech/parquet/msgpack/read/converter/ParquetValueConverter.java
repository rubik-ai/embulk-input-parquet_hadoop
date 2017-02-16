package studio.adtech.parquet.msgpack.read.converter;

import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
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
public class ParquetValueConverter extends ParquetGroupConverter {
    // Converters for each field.
    private final Converter[] fieldConverters;

    private InternalMap currentMap;

    // TODO: make configurable
    private boolean assumeBinaryIsString = false;
    private boolean assumeInt96IsTimestamp = true;

    public ParquetValueConverter(GroupType schema, ParentContainerUpdater updater) {
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
            Converter converter;
            switch (field.getRepetition()) {
                case OPTIONAL:
                    //
                    converter = convertField(field, update);
                    break;
                case REQUIRED:
                    converter = convertField(field, update);
                    break;
                case REPEATED:
                    // TODO: array type
                    converter = convertField(field, update);
                    break;
                default:
                    // TODO:
                    throw new ParquetSchemaException("");
            }
            fieldConverters[i++] = converter;
        }
    }

    Value getCurrentRecord() {
        return currentMap.build();
    }

    @Override
    public Converter getConverter(int fieldIndex) {
        return fieldConverters[fieldIndex];
    }

    @Override
    public void start() {
        int i = 0;
        for (Converter converter : fieldConverters) {
            ((HasParentContainerUpdater)converter).getUpdater().start();
            currentMap.set(i, ValueFactory.newNil());
            i += 1;
        }
    }

    @Override
    public void end() {
        for (Converter converter : fieldConverters) {
            ((HasParentContainerUpdater)converter).getUpdater().end();
        }
        getUpdater().set(currentMap.build());
    }

    @Override
    public ParentContainerUpdater getUpdater() {
        return updater;
    }

    protected Converter newConverter(Type parquetType, ParentContainerUpdater updater) {
        return convertField(parquetType, updater);
    }

    private Converter convertField(Type field, ParentContainerUpdater updater) {
        if (field.isPrimitive()) {
            return convertPrimitiveField(field.asPrimitiveType(), updater);
        } else {
            return convertGroupField(field.asGroupType(), updater);
        }
    }

    private Converter convertPrimitiveField(PrimitiveType field, final ParentContainerUpdater updater) {
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
                } else switch (originalType) {
                    case INT_8:
                        return new ParquetPrimitiveConverter(updater) {
                            @Override
                            public void addInt(int value) {
                                this.updater.setByte((byte)value);
                            }
                        };
                    case INT_16:
                        return new ParquetPrimitiveConverter(updater) {
                            @Override
                            public void addInt(int value) {
                                this.updater.setShort((short)value);
                            }
                        };
                    case INT_32:
                        return new ParquetPrimitiveConverter(updater);
                    case DATE:
                        // TODO: DateType
                        throw new ParquetSchemaException("Parquet type not yet supported: " + typeString);
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

            case INT64:
                if (originalType == null) {
                    return new ParquetPrimitiveConverter(updater);
                } else switch (originalType) {
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

            case INT96:
                // TODO: document assumeInt96IsTimestamp
                checkConversionRequirement(
                        assumeInt96IsTimestamp,
                        "INT96 is not supported unless it's interpreted as timestamp. " +
                                "Please try to set assumeInt96IsTimestamp to true.");
                return new ParquetPrimitiveConverter(updater) {
                    @Override
                    public void addBinary(Binary value) {
                        if (value.length() != 12) {
                            throw new AssertionError(
                                    "Timestamps (with nanoseconds) are expected to be stored in 12-byte long binaries, " +
                                    "but got a " + String.valueOf(value.length()) + "-byte binary.");
                        }

                        ByteBuffer buf = value.toByteBuffer().order(ByteOrder.LITTLE_ENDIAN);
                        long timeOfDayNanos = buf.getLong();
                        int julianDays = buf.getInt();

                        updater.setLong(DateTimeUtils.fromJulianDay(julianDays, timeOfDayNanos));
                    }
                };

            case BINARY:
                if (originalType == null) {
                    if (assumeBinaryIsString) {
                        return new ParquetStringConverter(updater);
                    } else {
                        return new ParquetPrimitiveConverter(updater);
                    }
                } else switch (originalType) {
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

            case FIXED_LEN_BYTE_ARRAY:
                if (originalType == null) {
                    throw new ParquetSchemaException("Illegal Parquet type: " + typeString);
                } else switch (originalType) {
                    case DECIMAL:
                        DecimalType decimal = DecimalType.create(field);
                        return new ParquetDecimalConverter.BinaryDictionaryAware(decimal.getPrecision(), decimal.getScale(), updater);
                    case INTERVAL:
                        throw new ParquetSchemaException("Parquet type not yet supported: " + typeString);
                    default:
                        throw new ParquetSchemaException("Illegal Parquet type: " + typeString);
                }

            default:
                throw new ParquetSchemaException("Illegal Parquet type: " + typeString);
        }
    }

    private Converter convertGroupField(GroupType field, final ParentContainerUpdater updater) {
        OriginalType originalType = field.getOriginalType();
        if (originalType == null) {
            return new ParquetValueConverter(field, new ParentContainerUpdater.Noop() {
                @Override
                public void set(Value value) {
                    updater.set(value);
                }
            });
        }

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

                ParentContainerUpdater.Noop arrayUpdater = new ParentContainerUpdater.Noop() {
                    @Override
                    public void set(Value value) {
                        ParquetValueConverter.this.updater.set(value);
                    }
                };

                if (isElementType(repeatedType, field.getName())) {
                    return new ParquetArrayConverter.A(repeatedType.asGroupType(), arrayUpdater);
                } else {
                    //Type elementType = repeatedType.asGroupType().getType(0);
                    //boolean optional = elementType.isRepetition(Type.Repetition.OPTIONAL);
                    return new ParquetArrayConverter.B(repeatedType.asGroupType(), arrayUpdater);
                }

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

                return new ParquetMapConverter(field, updater, keyType, valueType);

            default:
                throw new ParquetSchemaException("Unrecognized Parquet type: " + field);
        }

    }


    private static boolean isElementType(Type repeatedType, String parentName) {
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

    /**
     * Mutable fixed-length map.
     */
    private static class InternalMap {
        private final int numFields;
        private final Value[] kvs;

        public InternalMap(List<String> keys) {
            this.numFields = keys.size();
            this.kvs = new Value[numFields * 2];
            int i = 0;
            for (String key : keys) {
                kvs[i++] = ValueFactory.newString(key);
                kvs[i++] = ValueFactory.newNil();
            }
        }

        public void set(int index, Value value) {
            kvs[index * 2 + 1] = value;
        }

        public Value build() {
            return ValueFactory.newMap(kvs, false);
        }
    }

    /**
     * Updater used together with field converters within a [[ParquetValueConverter]].  It propagates
     * converted filed values to the `index`-th cell in `currentMap`.
     */
    private static final class InternalMapUpdater extends ParentContainerUpdater.Noop {
        private final InternalMap map;
        private final int index;

        InternalMapUpdater(InternalMap map, int index) {
            this.map = map;
            this.index = index;
        }

        @Override
        public void set(Value value) {
            map.set(index, value);
        }

        @Override
        public void setBoolean(boolean value) {
            map.set(index, ValueFactory.newBoolean(value));
        }

        @Override
        public void setByte(byte value) {
            map.set(index, ValueFactory.newInteger(value));
        }

        @Override
        public void setShort(short value) {
            map.set(index, ValueFactory.newInteger(value));
        }

        @Override
        public void setInt(int value) {
            map.set(index, ValueFactory.newInteger(value));
        }

        @Override
        public void setLong(long value) {
            map.set(index, ValueFactory.newInteger(value));
        }

        @Override
        public void setFloat(float value) {
            map.set(index, ValueFactory.newFloat(value));
        }

        @Override
        public void setDouble(double value) {
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
    private class ArrayConverter extends GroupConverter {
        private final GroupType schema;

        ArrayConverter(GroupType schema) {
            this.schema = schema;
        }

        @Override
        public Converter getConverter(int fieldIndex) {
            return null;
        }

        @Override
        public void start() {

        }

        @Override
        public void end() {

        }
    }

    private static class DecimalType {
        static final int MAX_INT_DIGITS = 9;
        static final int MAX_LONG_DIGITS = 18;

        static DecimalType create(PrimitiveType field) {
            return create(field, -1);
        }

        static DecimalType create(PrimitiveType field, int maxPrecision) {
            int precision = field.getDecimalMetadata().getPrecision();
            int scale = field.getDecimalMetadata().getScale();

            checkConversionRequirement(
                    (maxPrecision == -1) ||  (1 <= precision && precision <= maxPrecision),
                    "Invalid decimal precision: %s cannot store %d digits (max %d)",
                    field.getName(), precision, maxPrecision
            );

            return new DecimalType(precision, scale);
        }

        private final int precision;
        private final int scale;

        private DecimalType(int precision, int scale) {
            this.precision = precision;
            this.scale = scale;
        }

        public int getPrecision() {
            return precision;
        }

        public int getScale() {
            return scale;
        }
    }

    private static void checkConversionRequirement(boolean condition, String message, Object... args) {
        if (!condition) {
            throw new ParquetSchemaException(String.format(message, args));
        }
    }
}
