package us.dot.its.jpo.deduplicator.deduplicator;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.state.RocksDBConfigSetter;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.CompressionType;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import java.util.Map;

/**
 * Bounded memory configuration for RocksDB for all topologies.
 * <p>References:
 * <ul>
 *     <li><a href="https://docs.confluent.io/platform/current/streams/developer-guide/memory-mgmt.html#rocksdb">Confluent: RocksDB Memory Management</a></li>
 *     <li><a href="https://docs.confluent.io/platform/current/streams/developer-guide/config-streams.html#rocksdb-config-setter">Confluent: RocksDB Config Setter</a></li>
 * </ul></p>
 * <p>Configured using environment variables:
 * <dl>
 *     <dt>DEDUPLICATOR_ROCKSDB_TOTAL_OFF_HEAP_MEMORY</dt><dd>Total block cache size</dd>
 *     <dt>DEDUPLICATOR_ROCKSDB_INDEX_FILTER_BLOCK_RATIO</dt><dd>Fraction of the block cache to use for high priority blocks (index
 *     and filter blocks).</dd>
 *     <dt>DEDUPLICATOR_ROCKSDB_TOTAL_MEMTABLE_MEMORY</dt><dd>Write buffer size, include in block cache</dd>
 *     <dt>DEDUPLICATOR_ROCKSDB_BLOCK_SIZE</dt><dd>{@link org.rocksdb.BlockBasedTableConfig#blockSize()}, Default 4KB</dd>
 *     <dt>DEDUPLICATOR_ROCKSDB_N_MEMTABLES</dt><dd>{@link org.rocksdb.Options#maxWriteBufferNumber()}, Default 2</dd>
 *     <dt>DEDUPLICATOR_ROCKSDB_MEMTABLE_SIZE</dt><dd>{@link org.rocksdb.Options#writeBufferSize()}, Default 64MB</dd>
 * </dl>
 * </p>
 */

@Slf4j
public class BoundedMemoryRocksDBConfig implements RocksDBConfigSetter {

    private final static long TOTAL_OFF_HEAP_MEMORY;
    private final static double INDEX_FILTER_BLOCK_RATIO;
    private final static long TOTAL_MEMTABLE_MEMORY;

    // Block size: Default 4K
    private final static long BLOCK_SIZE;
    
    // MaxWriteBufferNumber: Default 2
    private final static int N_MEMTABLES;

    // WriteBufferSize: Default 64MB
    private final static long MEMTABLE_SIZE;
    private final static long KB = 1024L;
    private final static long MB = KB * KB;

    static {
        RocksDB.loadLibrary();
        // Initialize properties from env variables
        TOTAL_OFF_HEAP_MEMORY = getEnvLong("DEDUPLICATOR_ROCKSDB_TOTAL_OFF_HEAP_MEMORY", 128 * MB);
        INDEX_FILTER_BLOCK_RATIO = getEnvDouble("DEDUPLICATOR_ROCKSDB_INDEX_FILTER_BLOCK_RATIO", 0.1);
        TOTAL_MEMTABLE_MEMORY = getEnvLong("DEDUPLICATOR_ROCKSDB_TOTAL_MEMTABLE_MEMORY", 64 * MB);
        BLOCK_SIZE = getEnvLong("DEDUPLICATOR_ROCKSDB_BLOCK_SIZE", 4 * KB);
        N_MEMTABLES = getEnvInt("DEDUPLICATOR_ROCKSDB_N_MEMTABLES", 2);
        MEMTABLE_SIZE = getEnvLong("ROCKSDB_MEMTABLE_SIZE", 16 * MB);

        log.info("Initialized BoundedMemoryRocksDBConfig.  TOTAL_OFF_HEAP_MEMORY = {}, INDEX_FILTER_BLOCK_RATIO = {}," +
                " TOTAL_MEMTABLE_MEMORY = {}, BLOCK_SIZE = {}, N_MEMTABLES = {}, MEMTABLE_SIZE = {}",
                TOTAL_OFF_HEAP_MEMORY, INDEX_FILTER_BLOCK_RATIO, TOTAL_MEMTABLE_MEMORY, BLOCK_SIZE, N_MEMTABLES,
                MEMTABLE_SIZE);
    }

    // See #1 below
    private static final org.rocksdb.Cache cache
            = new org.rocksdb.LRUCache(TOTAL_OFF_HEAP_MEMORY, -1, false, INDEX_FILTER_BLOCK_RATIO);
    private static final org.rocksdb.WriteBufferManager writeBufferManager
            = new org.rocksdb.WriteBufferManager(TOTAL_MEMTABLE_MEMORY, cache);
    @Override
    public void setConfig(final String storeName, final Options options, final Map<String, Object> configs) {
        log.info("Setting RocksDB config for store {}", storeName);
        BlockBasedTableConfig tableConfig = (BlockBasedTableConfig) options.tableFormatConfig();

        // These three options in combination will limit the memory used by RocksDB to the size passed to the block
        // cache (TOTAL_OFF_HEAP_MEMORY)
        tableConfig.setBlockCache(cache);
        tableConfig.setCacheIndexAndFilterBlocks(true);
        options.setWriteBufferManager(writeBufferManager);

        // These options are recommended to be set when bounding the total memory
        // See #2 below
        tableConfig.setCacheIndexAndFilterBlocksWithHighPriority(true);
        tableConfig.setPinTopLevelIndexAndFilter(true);

        // See #3 below
        tableConfig.setBlockSize(BLOCK_SIZE);
        options.setMaxWriteBufferNumber(N_MEMTABLES);
        options.setWriteBufferSize(MEMTABLE_SIZE);

        // Enable compression (optional). Compression can decrease the required storage
        // and increase the CPU usage of the machine. For CompressionType values, see
        // https://javadoc.io/static/org.rocksdb/rocksdbjni/6.4.6/org/rocksdb/CompressionType.html.
        options.setCompressionType(CompressionType.LZ4_COMPRESSION);
        options.setTableFormatConfig(tableConfig);
        log.info("Rocksdb set table config: {}, options: {}", tableConfig, options);
    }
    @Override
    public void close(final String storeName, final Options options) {
        // Cache and WriteBufferManager should not be closed here, as the same objects are shared by every store instance.
    }

    private static long getEnvLong(String name, long defaultValue) {
        String strValue = getEnvString(name);
        if (strValue == null) return defaultValue;
        
        try {
            return Long.parseLong(strValue);
        } catch (NumberFormatException nfe) {
            log.error("Error parsing env variable to long {}, {}", name, strValue, nfe);
            return defaultValue;
        }
    }

    private static int getEnvInt(String name, int defaultValue) {
        String strValue = getEnvString(name);
        if (strValue == null) return defaultValue;
        try {
            return Integer.parseInt(strValue);
        } catch (NumberFormatException nfe) {
            log.error("Error parsing env variable to long {}, {}", name, strValue, nfe);
            return defaultValue;
        }
    }

    private static double getEnvDouble(String name, double defaultValue) {
        String strValue = getEnvString(name);
        if (strValue == null) return defaultValue;
        try {
            return Double.parseDouble(strValue);
        } catch (NumberFormatException nfe) {
            log.error("Error parsing env variable to long {}, {}", name, strValue, nfe);
            return defaultValue;
        }
    }

    private static String getEnvString(String name) {
        String strValue = System.getenv(name);
        if (strValue == null) {
            log.warn("Env variable {} is not set", name);
        }
        return strValue;
    }
}