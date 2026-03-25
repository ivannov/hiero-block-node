package org.hiero.block.node.cloud.archive;

import com.hedera.bucky.S3Client;
import com.hedera.bucky.S3ClientException;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.node.base.tar.TaredBlockIterator;
import org.hiero.block.node.spi.blockmessaging.BlockMessagingFacility;
import org.hiero.block.node.spi.blockmessaging.BlockSource;
import org.hiero.block.node.spi.blockmessaging.PersistedNotification;
import org.hiero.block.node.spi.historicalblocks.BlockAccessor;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import static java.lang.System.Logger.Level.TRACE;

class LiveBlockArchiveTask implements BlockArchiveTask, Iterator<BlockAccessor> {

    /// The logger for this class.
    private final System.Logger LOGGER = System.getLogger(getClass().getName());

    private final ArchiveCloudStorageConfig config;
    private final BlockMessagingFacility blockMessaging;

    private Path blocksDirectory;

    private final long startBlockNumber;
    private final long lastBlockNumber;
    private final long maxBlocksPerTask;

    private long currentBlockNumber;
    private final AtomicLong blocksWritten = new AtomicLong(0);
    private final Map<Long, CompletableFuture<BlockAccessor>> blocks = new ConcurrentHashMap<>();

    private S3Client s3Client;

    LiveBlockArchiveTask(ArchiveCloudStorageConfig config, BlockMessagingFacility blockMessaging, long startBlockNumber,
                         long maxBlocksPerTask) {
        this.config = config;
        this.blockMessaging = blockMessaging;
        this.startBlockNumber = startBlockNumber;
        this.maxBlocksPerTask = maxBlocksPerTask;
        this.lastBlockNumber = startBlockNumber + maxBlocksPerTask - 1;
        this.currentBlockNumber = startBlockNumber - 1;
    }

    void init() throws S3ClientException, IOException {
        blocksDirectory = Path.of(config.basePath());
        s3Client = new S3Client(config.regionName(), config.endpointUrl(), config.bucketName(),
            config.accessKey(), config.secretKey());
        TaredBlockIterator taredBlockIterator = new TaredBlockIterator(BlockAccessor.Format.ZSTD_PROTOBUF, this);
        s3Client.uploadFile(computeKey(), config.storageClass(), taredBlockIterator, "application/x-tar");
    }

    private String computeKey() {
        String truncated = String.format("%019d", startBlockNumber)
            .substring(0, 19 - config.groupingLevel());
        List<String> parts = new ArrayList<>();
        for (int i = 0; i < truncated.length(); i += 4) {
            parts.add(truncated.substring(i, Math.min(i + 4, truncated.length())));
        }
        parts.set(parts.size() - 1, String.valueOf(Long.parseLong(parts.getLast())));
        return String.join("/", parts) + ".tar";
    }

    @Override
    public ArchiveResult submit(BlockUnparsed block, long blockNumber) {
        if (blocksWritten.get() == maxBlocksPerTask) {
            return ArchiveResult.FINISHED;
        }
        if (blockNumber < startBlockNumber || blockNumber > lastBlockNumber) {
            return ArchiveResult.INVALID_BLOCK_NUMBER;
        }

        CompletableFuture
                .supplyAsync(() -> new SingleBlockStoreTask(blocksDirectory).createBlockArchive(block, blockNumber))
                .thenAccept(blockAccessor -> {
                    if (blockAccessor != null) {
                        blocks.computeIfAbsent(blockNumber, _ -> new CompletableFuture<>()).complete(blockAccessor);
                        currentBlockNumber = blockNumber;
                        blocks.remove(blockNumber);
                        blocksWritten.incrementAndGet();
                        LOGGER.log(TRACE, "Wrote block {0} to archive", blockNumber);
                        blockMessaging.sendBlockPersisted(new PersistedNotification(blockNumber, true, 1_000, BlockSource.CLOUD_ARCHIVE));
                    } else {
                        blockMessaging.sendBlockPersisted(new PersistedNotification(blockNumber, false, 1_000, BlockSource.CLOUD_ARCHIVE));
                    }
                });
        return ArchiveResult.SUCCESS;
    }

    @Override
    public boolean hasNext() {
        return blocksWritten.get() < maxBlocksPerTask;
    }

    @Override
    public BlockAccessor next() {
        return blocks.computeIfAbsent(currentBlockNumber, _ -> new CompletableFuture<>())
            .join();
    }

    public void close() {
        s3Client.close();
    }
}
