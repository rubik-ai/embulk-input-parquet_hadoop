package studio.adtech.parquet.msgpack.read.converter;

/**
 * @author Koji Agawa
 */
interface HasParentContainerUpdater {
    ParentContainerUpdater getUpdater();
}
