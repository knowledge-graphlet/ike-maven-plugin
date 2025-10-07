package dev.ikm.ds.plugin;

import dev.ikm.ds.rocks.tasks.ImportProtobufTask;
import dev.ikm.ds.rocks.RocksProvider;
import dev.ikm.tinkar.common.service.ServiceKeys;
import dev.ikm.tinkar.common.service.ServiceProperties;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;

import java.io.File;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

@Mojo(name = "load")
public class Load extends AbstractMojo {

    /**
     * Path to a Tinkar protobuf ZIP file to import (e.g. tink-*.zip)
     */
    @Parameter(property = "importFile", required = true)
    private File importFile;

    /**
     * Root directory where the data store root will be created. Defaults to project build directory "target".
     */
    @Parameter(property = "dataStoreRoot")
    private File dataStoreRoot;

    /**
     * Folder name to create under the data store root for the Rocks KB. Defaults to "RocksKb".
     */
    @Parameter(property = "folderName", defaultValue = "RocksKb")
    private String folderName;

    /** Optional: List of UUIDs (as strings) to watch during import. */
    @Parameter(property = "watchUuids")
    private Set<String> watchUuids;

    @Override
    public void execute() throws MojoExecutionException {
        if (importFile == null || !importFile.exists() || !importFile.isFile()) {
            throw new MojoExecutionException("importFile must point to an existing .zip file: " + importFile);
        }

        File root = dataStoreRoot != null ? dataStoreRoot : new File("target");
        File kbDir = new File(root, folderName);
        kbDir.mkdirs();

        getLog().info("Data store root: " + kbDir.getAbsolutePath());
        ServiceProperties.set(ServiceKeys.DATA_STORE_ROOT, kbDir);

        // Build watch list if provided
        Set<UUID> watchList = new HashSet<>();
        if (watchUuids != null) {
            for (String s : watchUuids) {
                try {
                    watchList.add(UUID.fromString(s));
                } catch (IllegalArgumentException e) {
                    getLog().warn("Ignoring invalid UUID in watchUuids: " + s);
                }
            }
        }

        try {
            // Open Rocks provider (like RocksNewController.start())
            RocksProvider provider = new RocksProvider();
            try {
                getLog().info("Importing entities from: " + importFile.getAbsolutePath());
                ImportProtobufTask importTask = new ImportProtobufTask(importFile, provider, watchList);
                importTask.compute();
                provider.save();
                getLog().info("Import complete.");
            } finally {
                try {
                    provider.close();
                } catch (Exception e) {
                    // ignore
                }
            }
        } catch (Exception e) {
            throw new MojoExecutionException("Failed to load Rocks KB from: " + importFile, e);
        }
    }
}
