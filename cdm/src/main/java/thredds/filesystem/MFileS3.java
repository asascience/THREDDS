
package thredds.filesystem;

import thredds.inventory.MFile;

import software.amazon.awssdk.services.s3.model.S3Object;

import javax.annotation.concurrent.ThreadSafe;
//import java.util.*;

@ThreadSafe
public class MFileS3 implements MFile {

    private final String bucket;
    private final S3Object object;
    private Object auxInfo;

    public MFileS3(String bucket, S3Object object) {
        this.bucket = bucket;
        this.object = object;
    }

    @Override
    public long getLastModified() {
        return object.lastModified().getEpochSecond();
    }

    @Override
    public long getLength() {
        return object.size().longValue();
    }

    @Override
    public boolean isDirectory() {
        return object.key().endsWith("/");
    }

    @Override
    public String getPath() {
        //return "/data/" + bucket + "/" + object.key().split("/", -1)[0];
        return "/data/" + bucket + "/" + object.key();
    }

    @Override
    public String getName() {
        //return object.key().split("/")[-1];
        return object.key();
    }

    @Override
    public MFile getParent() {
        //return new MFileOS(file.getParentFile());
        return null;
    }

    @Override
    public int compareTo(MFile o) {
        return getPath().compareTo( o.getPath());
    }

    @Override
    public Object getAuxInfo() {
        //return auxInfo;
        return null;
    }

    @Override
    public void setAuxInfo(Object auxInfo) {
        //this.auxInfo = auxInfo;
    }

}
