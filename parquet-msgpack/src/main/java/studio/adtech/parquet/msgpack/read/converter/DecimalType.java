package studio.adtech.parquet.msgpack.read.converter;

import org.apache.parquet.schema.PrimitiveType;

class DecimalType {
    static final int MAX_INT_DIGITS = 9;
    static final int MAX_LONG_DIGITS = 18;

    static DecimalType create(PrimitiveType field) {
        return create(field, -1);
    }

    static DecimalType create(PrimitiveType field, int maxPrecision) {
        int precision = field.getDecimalMetadata().getPrecision();
        int scale = field.getDecimalMetadata().getScale();

        if (!((maxPrecision == -1) || (1 <= precision && precision <= maxPrecision))) {
            throw new ParquetSchemaException(String.format(
                    "Invalid decimal precision: %s cannot store %d digits (max %d)", field.getName(), precision, maxPrecision));
        }

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
