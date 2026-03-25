package org.hiero.block.node.cloud.archive;

import com.hedera.pbj.runtime.io.buffer.Bytes;
import edu.umd.cs.findbugs.annotations.NonNull;
import org.hiero.block.node.spi.historicalblocks.BlockAccessor;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static java.lang.System.Logger.Level.INFO;

public class MyBlockAccessor implements BlockAccessor {

    private final System.Logger LOGGER = System.getLogger(getClass().getName());

    private final Path blockPath;
    private final long blockNumber;

    public MyBlockAccessor(Path blockPath, long blockNumber) {
        this.blockPath = blockPath;
        this.blockNumber = blockNumber;
    }

    @Override
    public long blockNumber() {
        return blockNumber;
    }

    @Override
    public Bytes blockBytes(@NonNull final Format format) {
        if (format != Format.ZSTD_PROTOBUF) {
            throw new IllegalArgumentException("Unsupported format: " + format);
        }
        try {
            return Bytes.wrap(Files.readAllBytes(blockPath));
        } catch (final IOException e) {
            final String message = "Failed to read block %d from file %s.".formatted(blockNumber, blockPath.toAbsolutePath().toString());
            LOGGER.log(INFO, message, e);
        }
    }

    @Override
    public void close() {
        try {
            Files.deleteIfExists(blockPath);
        } catch (IOException e) {
            final String message = "Failed to delete file %s for block %d.".formatted(blockPath.toAbsolutePath().toString(), blockNumber);
            LOGGER.log(INFO, message, e);
        }
    }

    @Override
    public boolean isClosed() {
        return Files.exists(blockPath);
    }
}
