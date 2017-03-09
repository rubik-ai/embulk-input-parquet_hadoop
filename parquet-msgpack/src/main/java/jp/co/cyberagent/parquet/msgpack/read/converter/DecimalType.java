/*
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

import org.apache.parquet.schema.PrimitiveType;

class DecimalType
{
    static final int MAX_INT_DIGITS = 9;
    static final int MAX_LONG_DIGITS = 18;

    static DecimalType create(PrimitiveType field)
    {
        return create(field, -1);
    }

    static DecimalType create(PrimitiveType field, int maxPrecision)
    {
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

    private DecimalType(int precision, int scale)
    {
        this.precision = precision;
        this.scale = scale;
    }

    public int getPrecision()
    {
        return precision;
    }

    public int getScale()
    {
        return scale;
    }
}
