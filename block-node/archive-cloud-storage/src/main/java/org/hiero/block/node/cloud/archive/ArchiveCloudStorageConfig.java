package org.hiero.block.node.cloud.archive;

import com.swirlds.config.api.ConfigData;
import com.swirlds.config.api.ConfigProperty;
import org.hiero.block.node.base.Loggable;

@ConfigData("cloud-archive")
public record ArchiveCloudStorageConfig(
    @Loggable @ConfigProperty(defaultValue = "5") int groupingLevel,
    @Loggable @ConfigProperty(defaultValue = "") String endpointUrl,
    @Loggable @ConfigProperty(defaultValue = "block-node-archive") String bucketName,
    @Loggable @ConfigProperty(defaultValue = "cloud") String basePath,
    @Loggable @ConfigProperty(defaultValue = "STANDARD") String storageClass,
    @Loggable @ConfigProperty(defaultValue = "us-east-1") String regionName,
    @ConfigProperty(defaultValue = "") String accessKey,
    @ConfigProperty(defaultValue = "") String secretKey) {
}
