// SPDX-License-Identifier: Apache-2.0
module org.hiero.block.node.archive.cloud.storage {
import org.hiero.block.node.cloud.archive.ArchiveCloudStoragePlugin;

module org.hiero.block.node.cloud.archive {
    requires transitive org.hiero.block.node.spi;

    provides org.hiero.block.node.spi.BlockNodePlugin with
            org.hiero.block.node.archive.cloud.storage.ArchiveCloudStoragePlugin;
}
