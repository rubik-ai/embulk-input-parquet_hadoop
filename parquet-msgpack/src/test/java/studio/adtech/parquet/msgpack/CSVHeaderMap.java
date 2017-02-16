package studio.adtech.parquet.msgpack;

import org.apache.commons.csv.CSVFormat;

import java.util.ArrayList;
import java.util.List;

public class CSVHeaderMap {
    private final Entry[] headers;

    public static CSVHeaderMap.Builder builder() {
        return new Builder();
    }

    private CSVHeaderMap(Builder builder) {
        this.headers = builder.headers.toArray(new Entry[builder.headers.size()]);
    }

    public CSVFormat injectHeaderFormat(CSVFormat format) {
        String[] names = new String[headers.length];
        int i = 0;
        for (Entry header : headers) {
            names[i] = header.name;
            i += 1;
        }
        return format.withHeader(names);
    }

    public Entry[] entries() {
        return headers;
    }

    public CSVColumnWriter getColumnWriterAt(int index) {
        return headers[index].writer;
    }

    public static class Builder {
        private final List<Entry> headers = new ArrayList<>();
        private int index = 0;

        public Builder add(String name, CSVColumnWriter writer) {
            headers.add(new Entry(index, name, writer));
            index += 1;
            return this;
        }

        public CSVHeaderMap build() {
            return new CSVHeaderMap(this);
        }
    }

    public static class Entry {
        private final int index;
        private final String name;
        private final CSVColumnWriter writer;

        public Entry(int index, String name, CSVColumnWriter writer) {
            this.index = index;
            this.name = name;
            this.writer = writer;
        }

        public int getIndex() {
            return index;
        }

        public String getName() {
            return name;
        }

        public CSVColumnWriter getWriter() {
            return writer;
        }
    }
}
