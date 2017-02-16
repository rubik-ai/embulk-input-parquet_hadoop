package studio.adtech.parquet.msgpack;

import org.codehaus.jackson.JsonGenerator;

import java.io.IOException;

public interface CSVColumnWriter {
    void write(JsonGenerator gen, String value) throws IOException;

    CSVColumnWriter NUMBER = new CSVColumnWriter() {
        @Override
        public void write(JsonGenerator gen, String value) throws IOException {
            gen.writeNumber(value);
        }
    };

    CSVColumnWriter STRING = new CSVColumnWriter() {
        @Override
        public void write(JsonGenerator gen, String value) throws IOException {
            gen.writeString(value);
        }
    };
}
