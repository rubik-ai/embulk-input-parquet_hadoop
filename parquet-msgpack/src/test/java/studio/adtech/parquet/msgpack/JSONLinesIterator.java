package studio.adtech.parquet.msgpack;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;

import java.io.File;
import java.io.IOException;

public class JSONLinesIterator implements JSONIterator {
    private final LineIterator inner;

    public static JSONLinesIterator fromResource(String name) {
        File file = new File(Thread.currentThread().getContextClassLoader().getResource(name).getPath());
        return new JSONLinesIterator(file);
    }

    public JSONLinesIterator(File file) {
        try {
            inner = FileUtils.lineIterator(file);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean hasNext() {
        return inner.hasNext();
    }

    @Override
    public String next() {
        return inner.next();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws IOException {
        inner.close();
    }
}
