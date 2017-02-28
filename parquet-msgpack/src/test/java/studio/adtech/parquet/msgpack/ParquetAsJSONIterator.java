package studio.adtech.parquet.msgpack;

import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class ParquetAsJSONIterator implements JSONIterator {
    private final ParquetIterator inner;

    public static ParquetAsJSONIterator fromResource(String name) {
        Path path = new Path(Thread.currentThread().getContextClassLoader().getResource(name).getPath());
        return new ParquetAsJSONIterator(path);
    }

    public ParquetAsJSONIterator(Path path) {
        inner = new ParquetIterator(path);
    }

    @Override
    public boolean hasNext() {
        return inner.hasNext();
    }

    @Override
    public String next() {
        return inner.next().toString();
    }

    @Override
    public void remove() {
        inner.remove();
    }

    @Override
    public void close() throws IOException {
        inner.close();
    }
}
