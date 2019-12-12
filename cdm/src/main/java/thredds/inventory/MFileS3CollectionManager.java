
package thredds.inventory;

import thredds.featurecollection.FeatureCollectionConfig;
import thredds.filesystem.ControllerS3;
import thredds.inventory.MFileCollectionManager;

import java.io.IOException;
import java.util.*;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Manage Collections of MS3Files.
 */
@ThreadSafe
public class MFileS3CollectionManager extends MFileCollectionManager {

    static public MController getController() {
        if (null == controller) controller = new thredds.filesystem.ControllerS3();
        return controller;
    }

    // called from Aggregation, Fmrc, FeatureDatasetFactoryManager
    static public MFileS3CollectionManager open(String collectionName, String collectionSpec, String olderThan, Formatter errlog) throws IOException {
        MFileS3CollectionManager cm = new MFileS3CollectionManager(collectionName, collectionSpec, olderThan, errlog);
        cm.setController(new thredds.filesystem.ControllerS3());
        return cm;
    }


    private MFileS3CollectionManager(String collectionName, String collectionSpec, String olderThan, Formatter errlog) {
        super(collectionName, collectionSpec, olderThan, errlog);
        setController(new thredds.filesystem.ControllerS3());
    }

    public MFileS3CollectionManager(FeatureCollectionConfig config, Formatter errlog, org.slf4j.Logger logger) {
        super(config, errlog, logger);
        setController(new thredds.filesystem.ControllerS3());
    }

}
