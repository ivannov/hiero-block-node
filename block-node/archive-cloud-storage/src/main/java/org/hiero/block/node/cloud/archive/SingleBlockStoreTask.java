package org.hiero.block.node.cloud.archive;

import com.github.luben.zstd.ZstdOutputStream;
import com.hedera.pbj.runtime.io.stream.WritableStreamingData;
import org.hiero.block.internal.BlockUnparsed;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

import static java.lang.System.Logger.Level.INFO;
import static java.lang.System.Logger.Level.TRACE;
import static java.lang.System.Logger.Level.WARNING;

class SingleBlockStoreTask {

    /// The logger for this class.
    private final System.Logger LOGGER = System.getLogger(getClass().getName());

    private final Path blocksDirectory;

    SingleBlockStoreTask(Path blocksDirectory) {
        this.blocksDirectory = blocksDirectory;
    }

    MyBlockAccessor createBlockArchive(BlockUnparsed block, long blockNumber) {
        final Path blockPath = blocksDirectory.resolve(blockNumber + ".blk.zstd");
        try (final WritableStreamingData streamingData = new WritableStreamingData(new BufferedOutputStream(
            new ZstdOutputStream(Files.newOutputStream(blockPath)), 16384))) {
            BlockUnparsed.PROTOBUF.write(block, streamingData);
            streamingData.flush();
            LOGGER.log(TRACE, "Wrote verified block {0} to file {1}", blockNumber, blockPath.toAbsolutePath());
            return new MyBlockAccessor(blockPath, blockNumber);
        } catch (final IOException e) {
            final String message = "Failed to write file for block %d due to %s".formatted(blockNumber, e);
            LOGGER.log(INFO, message, e);
            return null;
        }
    }
}
