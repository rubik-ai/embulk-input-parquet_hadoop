package org.embulk.input.parquet_hadoop.read.converter;

/**
 * @author Koji Agawa
 */
interface HasParentContainerUpdater {
    ParentContainerUpdater getUpdater();
}
