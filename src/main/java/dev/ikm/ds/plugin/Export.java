package dev.ikm.ds.plugin;

import dev.ikm.tinkar.entity.EntityCountSummary;
import dev.ikm.tinkar.entity.EntityService;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;

import java.io.File;
import java.util.concurrent.CompletableFuture;

/**
 * Export all entities from a Rocks-backed datastore to a Protobuf file.
 *
 * Usage:
 *   mvn dev.ikm:ike-maven-plugin:export -Doutput=/path/to/export.tink
 *
 * Optional parameters:
 *   -DfromEpoch=... -DtoEpoch=...      (temporal export)
 */
@Mojo(name = "export")
public class Export extends AbstractMojo {

    /**
     * Output file for the export.
     */
    @Parameter(property = "output", required = true)
    private File output;

    /**
     * Optional: temporal export window start (inclusive), epoch millis.
     */
    @Parameter(property = "fromEpoch", required = false)
    private Long fromEpoch;

    /**
     * Optional: temporal export window end (exclusive), epoch millis.
     */
    @Parameter(property = "toEpoch", required = false)
    private Long toEpoch;

    @Override
    public void execute() throws MojoExecutionException {
        try {
            if (output == null) {
                throw new MojoExecutionException("Output file must be specified via -Doutput=...");
            }
            output.getParentFile().mkdirs();

            CompletableFuture<EntityCountSummary> task;
            if (fromEpoch != null && toEpoch != null) {
                getLog().info("Starting temporal export: " + fromEpoch + " .. " + toEpoch + " -> " + output.getAbsolutePath());
                task = EntityService.get().temporalExport(output, fromEpoch, toEpoch);
            } else {
                getLog().info("Starting full export -> " + output.getAbsolutePath());
                task = EntityService.get().fullExport(output);
            }

            EntityCountSummary result = task.join();
            getLog().info("Export complete: " + result);
        } catch (Exception e) {
            throw new MojoExecutionException("Export failed", e);
        }
    }
}