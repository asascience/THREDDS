
package thredds.filesystem;

import thredds.inventory.CollectionConfig;
import thredds.inventory.MController;
import thredds.inventory.MFile;

import software.amazon.awssdk.core.pagination.sync.SdkIterable;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;

import javax.annotation.concurrent.ThreadSafe;
import java.io.File;
import java.util.*;

/**
 * Implements an MController without caching, reading from S3
 * recheck is ignored (always true)
 */

@ThreadSafe
public class ControllerS3 implements MController {

    static private org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ControllerS3.class);

    public ControllerS3() {
    }

    @Override
    public Iterator<MFile> getInventoryAll(CollectionConfig mc, boolean recheck) {

        String path = mc.getDirectoryName(); // as configured in Catalog
        if (path.startsWith("s3fs:")) { // TODO: configurable mount prefix
            path = path.replaceFirst("s3fs:", "/data");
        }
        String[] _arr = path.replace("/data/", "").split("/", 2);
        String bucket = _arr[0];
        String prefix = _arr[1];
        if (!prefix.endsWith("/")) {
            prefix = prefix + "/";
        }
        logger.debug(path + ": (bucket) " + bucket + " (prefix) " + prefix );
        return new ListObjectsV2Iterator(bucket, prefix);

    }

    @Override
    public Iterator<MFile> getInventoryTop(CollectionConfig mc, boolean recheck) {

        String path = mc.getDirectoryName(); // as configured in Catalog
        if (path.startsWith("s3fs:")) { // TODO: configurable mount prefix
            path = path.replaceFirst("s3fs:", "/data");
        }
        String[] _arr = path.replace("/data/", "").split("/", 2);
        String bucket = _arr[0];
        String prefix = _arr[1];
        if (!prefix.endsWith("/")) {
            prefix = prefix + "/";
        }
        logger.debug(path + ": (bucket) " + bucket + " (prefix) " + prefix );
        return new ListObjectsV2Iterator(bucket, prefix); // TODO remove directories

    }

    public Iterator<MFile> getSubdirs(CollectionConfig mc, boolean recheck) {
        throw new UnsupportedOperationException();
    }

    public void close() {
    } // NOOP


    private static class ListObjectsV2Iterator implements Iterator<MFile> {

        private String bucket;
        private Iterator<S3Object> contents;
        private Region region = Region.US_EAST_1;
        private S3Client s3 = S3Client.builder().region(region).build();

        ListObjectsV2Iterator(String bucket, String prefix) {
            this.bucket = bucket;
            ListObjectsV2Request request = ListObjectsV2Request.builder()
                    .bucket(bucket)
                    .prefix(prefix)
                    .delimiter("/")
                    .maxKeys(1000)
                    .build();
            ListObjectsV2Iterable responses = s3.listObjectsV2Paginator(request);
            contents = responses.contents().iterator();
            if (!contents.hasNext()) { // null on i/o error
                logger.warn("I/O error on S3 bucket: " + bucket + " prefix: " + prefix);
                throw new IllegalStateException("S3 ListObjectsV2 returned null on " + bucket + "/" + prefix);
            }
        }

        public boolean hasNext() {
            return contents.hasNext();
        }

        public MFile next() {
            S3Object object = contents.next();
            return new MFileS3(bucket, object);
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }

    }

}
