// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.cloud.archive;

import static java.lang.System.Logger.Level.DEBUG;
import static java.lang.System.Logger.Level.INFO;
import static java.util.Objects.requireNonNull;

import com.hedera.bucky.S3ClientException;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.BlockNodePlugin;
import org.hiero.block.node.spi.ServiceBuilder;
import org.hiero.block.node.spi.blockmessaging.BlockNotificationHandler;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification;

import java.io.IOException;

/// A block node plugin that stores verified blocks in cloud storage aggregated into large `tar` archives.
///
/// On each [org.hiero.block.node.spi.blockmessaging.VerificationNotification], a
/// `SingleBlockStoreTask` compresses the block with ZStandard and passes it to the active
/// `BlockArchiveTask`, which streams the data directly to the remote `tar` file in block-number
/// order. No local staging occurs. A
/// [org.hiero.block.node.spi.blockmessaging.PersistedNotification] is published per block
/// only after durable remote storage is confirmed.
///
/// When blocks arrive out of order or an operation fails, the plugin initiates recovery via an
/// `ArchiveRecoveryTask` that consolidates any temporary `tar` files and resumes normal archiving.
public class ArchiveCloudStoragePlugin implements BlockNodePlugin, BlockNotificationHandler {

    /// The logger for this class.
    private final System.Logger LOGGER = System.getLogger(getClass().getName());

    private BlockNodeContext context;
    private ArchiveCloudStorageConfig config;

    private LiveBlockArchiveTask liveArchiveTask = null;


    @Override
    public void init(BlockNodeContext context, ServiceBuilder serviceBuilder) {
        this.context = requireNonNull(context);
        this.config = context.configuration().getConfigData(ArchiveCloudStorageConfig.class);
    }

    /// {@inheritDoc}
    ///
    /// Starts the archive cloud storage plugin.
    @Override
    public void start() {
        LOGGER.log(DEBUG, "Archive cloud storage plugin started");
    }

    @Override
    public void handleVerification(VerificationNotification notification) {
        try {
            if (liveArchiveTask == null) {
                startArchiveTask(notification.blockNumber());
            }

            final BlockArchiveTask.ArchiveResult result = liveArchiveTask.submit(notification.block(), notification.blockNumber());
            // TODO handle FAILED and INVALID_BLOCK_NUMBER
            if (result == BlockArchiveTask.ArchiveResult.FINISHED) {
                liveArchiveTask.close();
                startArchiveTask(notification.blockNumber());
                liveArchiveTask.submit(notification.block(), notification.blockNumber());
            }
        } catch (S3ClientException | IOException e) {
            LOGGER.log(INFO, "Failed to initialize archiving task.", e);
        }
    }

    private void startArchiveTask(long blockNumber) throws S3ClientException, IOException {
        final long groupSize = Math.powExact(10, config.groupingLevel());
        final long startBlock = (blockNumber / groupSize) * groupSize;
        liveArchiveTask = new LiveBlockArchiveTask(config, context.blockMessaging(), startBlock, groupSize);
        liveArchiveTask.init();
    }

}
