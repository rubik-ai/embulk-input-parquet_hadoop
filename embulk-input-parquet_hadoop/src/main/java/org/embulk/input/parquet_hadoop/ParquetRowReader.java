package org.embulk.input.parquet_hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.UnmaterializableRecordCounter;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;
import org.embulk.spi.Exec;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ParquetRowReader<T> {
    private static final Logger logger = Exec.getLogger(ParquetRowReader.class);

    private final Path filePath;
    private final ParquetFileReader reader;
    private final long total;
    private final ColumnIOFactory columnIOFactory;
    private final RecordMaterializer<T> recordConverter;
    private final MessageType requestedSchema;
    private final MessageType fileSchema;
    private final UnmaterializableRecordCounter unmaterializableRecordCounter;

    private long current = 0;
    private long totalCountLoadedSoFar = 0;
    private int currentBlock = -1;
    private RecordReader<T> recordReader;

    // TODO: make configurable ?
    private static final boolean strictTypeChecking = true;
    private static final FilterCompat.Filter filter = FilterCompat.NOOP;

    public ParquetRowReader(Configuration configuration, Path filePath, ReadSupport<T> readSupport) throws IOException {
        this.filePath = filePath;

        ParquetMetadata parquetMetadata = ParquetFileReader.readFooter(configuration, filePath, ParquetMetadataConverter.NO_FILTER);
        List<BlockMetaData> blocks = parquetMetadata.getBlocks();

        FileMetaData fileMetadata = parquetMetadata.getFileMetaData();
        this.fileSchema = fileMetadata.getSchema();
        Map<String, String> keyValueMetadata = fileMetadata.getKeyValueMetaData();
        ReadSupport.ReadContext readContext = readSupport.init(new InitContext(
                configuration, toSetMultiMap(keyValueMetadata), fileSchema));
        this.columnIOFactory = new ColumnIOFactory(fileMetadata.getCreatedBy());

        this.requestedSchema = readContext.getRequestedSchema();
        this.recordConverter = readSupport.prepareForRead(
                configuration, fileMetadata.getKeyValueMetaData(), fileSchema, readContext);

        List<ColumnDescriptor> columns = requestedSchema.getColumns();

        reader = new ParquetFileReader(configuration, fileMetadata, filePath, blocks, columns);

        long total = 0;
        for (BlockMetaData block : blocks) {
            total += block.getRowCount();
        }
        this.total = total;

        this.unmaterializableRecordCounter = new UnmaterializableRecordCounter(configuration, total);
        logger.info("ParquetRowReader initialized will read a total of " + total + " records.");
    }

    private void checkRead() throws IOException {
        if (current == totalCountLoadedSoFar) {
            PageReadStore pages = reader.readNextRowGroup();
            if (pages == null) {
                throw new IOException("expecting more rows but reached last block. Read " + current + " out of " + total);
            }

            MessageColumnIO columnIO = columnIOFactory.getColumnIO(requestedSchema, fileSchema, strictTypeChecking);
            recordReader = columnIO.getRecordReader(pages, recordConverter, filter);
            totalCountLoadedSoFar += pages.getRowCount();
            ++ currentBlock;
        }
    }

    /**
     * @return the next record or null if finished
     * @throws IOException
     * @throws ParquetDecodingException
     */
    public T read() throws IOException {
        T currentValue = null;
        boolean recordFound = false;
        while (!recordFound) {
            // no more records left
            if (current >= total) {
                return null;
            }

            try {
                checkRead();
                current++;

                try {
                    currentValue = recordReader.read();
                } catch (RecordMaterializer.RecordMaterializationException e) {
                    // this might throw, but it's fatal if it does.
                    unmaterializableRecordCounter.incErrors(e);
                    logger.debug("skipping a corrupt record");
                    continue;
                }

                if (recordReader.shouldSkipCurrentRecord()) {
                    // this record is being filtered via the filter2 package
                    logger.debug("skipping record");
                    continue;
                }

                if (currentValue == null) {
                    // only happens with FilteredRecordReader at end of block
                    current = totalCountLoadedSoFar;
                    logger.debug("filtered record reader reached end of block");
                    continue;
                }

                recordFound = true;

                logger.debug("read value: {}", currentValue);
            } catch (RuntimeException e) {
                throw new ParquetDecodingException(
                        String.format("Can not read value at %d in block %d in file %s", current, currentBlock, filePath), e);
            }
        }

        return currentValue;
    }

    public void close() throws IOException {
        reader.close();
    }

    private static <K, V> Map<K, Set<V>> toSetMultiMap(Map<K, V> map) {
        Map<K, Set<V>> setMultiMap = new HashMap<>();
        for (Map.Entry<K, V> entry : map.entrySet()) {
            Set<V> set = new HashSet<>();
            set.add(entry.getValue());
            setMultiMap.put(entry.getKey(), Collections.unmodifiableSet(set));
        }
        return Collections.unmodifiableMap(setMultiMap);
    }
}
