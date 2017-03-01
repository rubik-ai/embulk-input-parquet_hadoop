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
