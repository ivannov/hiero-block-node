package org.hiero.block.node.cloud.archive;

import org.hiero.block.internal.BlockUnparsed;

interface BlockArchiveTask {

    enum ArchiveResult {
        SUCCESS,
        INVALID_BLOCK_NUMBER,
        FAILED,
        FINISHED
    }

    ArchiveResult submit(final BlockUnparsed block, final long blockNumber);

}
