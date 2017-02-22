package studio.adtech.parquet.msgpack;

import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetReader;
import org.msgpack.value.Value;
import studio.adtech.parquet.msgpack.read.MessagePackReadSupport;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;

public class ParquetIterator implements Iterator<Value>, Closeable {
    private final ParquetReader<Value> reader;
    private Value item;

    public static ParquetIterator fromResource(String name) {
        Path path = new Path(Thread.currentThread().getContextClassLoader().getResource(name).getPath());
        return new ParquetIterator(path);
    }

    public ParquetIterator(Path path) {
        try {
            reader = ParquetReader.builder(new MessagePackReadSupport(), path).build();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        item = read();
    }

    private Value read() {
        try {
            return reader.read();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean hasNext() {
        return item != null;
    }

    @Override
    public Value next() {
        Value ret = this.item;
        this.item = read();
        return ret;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }
}
