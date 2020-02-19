package com.purestorage.reddot;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;

import java.util.Iterator;

public class DeleteBucket {

    private String region;
    private String bucket;

    public DeleteBucket(String region, String bucket) {
        this.region = region;
        this.bucket = bucket;
    }

    public static void main(String[] args) {

        if (args.length < 2) {
            println("Usage: java com.purestorage.reddot.DeleteBucket regionName bucketName");
            System.exit(-1);
        }

        DeleteBucket deleteBucket = new DeleteBucket(args[0], args[1]);
        deleteBucket.purge();
    }

    private static void println(String line) {
        System.out.println(line);
    }

    /**
     * Purge a S3 bucket. Delete all objects and object versions in the bucket.
     *
     */
    public void purge() {
        try {

            AmazonS3 s3 = AmazonS3ClientBuilder.standard()
                    .withCredentials(new ProfileCredentialsProvider())
                    .withRegion(region)
                    .build();

            // Delete all objects
            deleteObjects(s3);

            // Delete all object versions (required for versioned buckets).
            deleteVersions(s3);

            // After all objects and object versions are deleted, delete the bucket.
            s3.deleteBucket(bucket);

        } catch (AmazonServiceException e) {
            // The call was transmitted successfully, but Amazon S3 couldn't process
            // it, so it returned an error response.
            e.printStackTrace();
        } catch (SdkClientException e) {
            // Amazon S3 couldn't be contacted for a response, or the client couldn't
            // parse the response from Amazon S3.
            e.printStackTrace();
        }
    }


    /**
     * Delete all objcets.
     *
     * @param s3 S3 client
     */
    void deleteObjects(AmazonS3 s3) {
        // Delete all objects from the bucket. This is sufficient
        // for unversioned buckets. For versioned buckets, when you attempt to delete objects, Amazon S3 inserts
        // delete markers for all objects, but doesn't delete the object versions.
        // To delete objects from versioned buckets, delete all of the object versions before deleting
        // the bucket (see below for an example).
        ObjectListing objectListing = s3.listObjects(bucket);
        while (true) {
            Iterator<S3ObjectSummary> objIter = objectListing.getObjectSummaries().iterator();
            while (objIter.hasNext()) {
                s3.deleteObject(bucket, objIter.next().getKey());
            }

            // If the bucket contains many objects, the listObjects() call
            // might not return all of the objects in the first listing. Check to
            // see whether the listing was truncated. If so, retrieve the next page of objects
            // and delete them.
            if (objectListing.isTruncated()) {
                objectListing = s3.listNextBatchOfObjects(objectListing);
            } else {
                break;
            }
        }
    }

    /**
     * Delete all object versions
     *
     * @param s3 S3 client
     */
    void deleteVersions(AmazonS3 s3) {
        VersionListing versionList = s3.listVersions(new ListVersionsRequest().withBucketName(bucket));
        while (true) {
            Iterator<S3VersionSummary> versionIter = versionList.getVersionSummaries().iterator();
            while (versionIter.hasNext()) {
                S3VersionSummary vs = versionIter.next();
                s3.deleteVersion(bucket, vs.getKey(), vs.getVersionId());
            }

            if (versionList.isTruncated()) {
                versionList = s3.listNextBatchOfVersions(versionList);
            } else {
                break;
            }
        }
    }
}

