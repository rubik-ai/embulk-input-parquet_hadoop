package org.embulk.input.parquet_hadoop;

/**
 * Set the context class loader to plugin's class loader.
 *
 * {@link org.apache.hadoop.fs.FileSystem#loadFileSystems()} loads FileSystem implementation via
 * {@link java.util.ServiceLoader}.
 * It's look up services via system class loader if context class loader is null.
 * However system class loader failed to look up FileSystem implementations because
 * hadoop jars is not in classpath of system class loader.
 * So we need to set context class loader to plugins' class loader.
 */
class PluginClassLoaderScope implements AutoCloseable {
    private static final ClassLoader PLUGIN_CLASS_LOADER =
            ParquetHadoopInputPlugin.class.getClassLoader();

    private final ClassLoader original;

    public PluginClassLoaderScope() {
        Thread current = Thread.currentThread();
        this.original = current.getContextClassLoader();
        Thread.currentThread().setContextClassLoader(PLUGIN_CLASS_LOADER);
    }

    @Override
    public void close() {
        Thread.currentThread().setContextClassLoader(original);
    }
}
