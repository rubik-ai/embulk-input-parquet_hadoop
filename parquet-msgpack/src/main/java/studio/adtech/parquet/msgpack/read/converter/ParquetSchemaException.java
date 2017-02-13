package studio.adtech.parquet.msgpack.read.converter;

/**
 * @author Koji Agawa
 */
public class ParquetSchemaException extends RuntimeException {
    ParquetSchemaException(String message) {
        super(message);
    }

    ParquetSchemaException(String message, Throwable cause) {
        super(message, cause);
    }
}
