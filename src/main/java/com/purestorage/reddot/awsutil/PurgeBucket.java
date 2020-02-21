package com.purestorage.reddot.awsutil;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.services.s3.model.DeleteObjectsRequest.KeyVersion;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.*;

public class PurgeBucket {

    private String region;
    private String bucket;
    private static int BULK_SIZE = 1000;
    private static int MAX_CONCURRENCY = 20;

    public PurgeBucket(String region, String bucket) {
        this.region = region;
        this.bucket = bucket;
    }

    public static void main(String[] args) {

        if (args.length < 2) {
            println("Usage: java com.purestorage.reddot.PurgeBucket regionName bucketName");
            System.exit(-1);
        }

        PurgeBucket purgeBucket = new PurgeBucket(args[0], args[1]);
        purgeBucket.purge();
    }

    /**
     * Purge a S3 bucket. Delete all objects and object versions in the bucket.
     *
     */
    public void purge() {

        AmazonS3 s3 = AmazonS3ClientBuilder.standard()
                .withCredentials(new ProfileCredentialsProvider())
                .withRegion(region)
                .build();

        // List top level directories
        ListObjectsRequest request = new ListObjectsRequest()
                                        .withBucketName(bucket)
                                        .withDelimiter("/");
        ObjectListing listing = s3.listObjects(request);
        List<String> topDirectories = listing.getCommonPrefixes();
        int numThreads = topDirectories.size() < MAX_CONCURRENCY ? topDirectories.size() : MAX_CONCURRENCY;
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);

        // For each directory, create a task to delete objects in it.
        List<Callable<Integer>> tasks = new ArrayList();
        for (String topDir : topDirectories) {
            Callable<Integer> deleteTask = () -> {
                return deleteVersions(topDir);
            };

            tasks.add(deleteTask);
        }
        int totalTasks = tasks.size();
        println(String.format("Found %d top directories to delete. Using %d threads.", totalTasks, numThreads));

        // submit tasks
        try {
            List<Future<Integer>> futures = executor.invokeAll(tasks);

            // Report progress
            boolean deleting = true;
            while (deleting) {
                int doneTasks = 0;
                TimeUnit.SECONDS.sleep(1);
                for (Future<Integer> future: futures
                ) {
                    doneTasks += future.isDone() ? 1 : 0;
                }
                println(String.format("Completing top directory deletion tasks %d/%d ...", doneTasks, totalTasks));
                deleting = doneTasks == totalTasks ? false : true;
            }

            // All done
            int deleted = 0;
            for (Future<Integer> future: futures
            ) {
                deleted += future.get();
            }

            // do not forget non-directory objects under root directory
            println("Deleting non-directory objects under root directory.");
            deleted += deleteVersions(s3, "");

            println("All deletion completed. Total number of object-version deleted: " + deleted);

        } catch (InterruptedException e) {
            throw new IllegalStateException("Execution interrupted", e);
        } catch (ExecutionException e) {
            throw new IllegalStateException("Execution failed", e);
        } finally {
            executor.shutdownNow();
        }

        // After all objects and object versions are deleted, delete the bucket.
        println("Deleting bucket: " + bucket);
        s3.deleteBucket(bucket);

        println("Finished purging bucket: " + bucket);
    }

    private static void println(String line) {
        System.out.println(line);
    }


    int deleteVersions(String prefix) {
        AmazonS3 s3 = AmazonS3ClientBuilder.standard()
                .withCredentials(new ProfileCredentialsProvider())
                .withRegion(region)
                .build();

        return deleteVersions(s3, prefix);
    }

    /**
     * Delete all object versions
     *
     * @param s3 S3 client
     * @param prefix object prefix to delete
     * @return number of object versions deleted.
     */
    int deleteVersions(AmazonS3 s3, String prefix) {
        println(String.format("Deleting object versions in %s, prefix=%s", bucket, prefix));
        int counter = 0;

        try {
            VersionListing versionList = s3.listVersions(new ListVersionsRequest()
                    .withBucketName(bucket)
                    .withPrefix(prefix));

            List<KeyVersion> keys = new ArrayList();

            while (true) {
                Iterator<S3VersionSummary> versionIter = versionList.getVersionSummaries().iterator();
                while (versionIter.hasNext()) {
                    S3VersionSummary vs = versionIter.next();
                    keys.add(new KeyVersion(vs.getKey(), vs.getVersionId()));
                    counter++;
                    if (counter % BULK_SIZE == 0) {
                        bulkDelete(s3, keys);
                    }
                    int perProgress = BULK_SIZE * 10;
                    if (counter % perProgress == 0) {
                        println(String.format("%s: %d objects deleted under %s", Thread.currentThread().getName(), counter, prefix));
                    }
                }

                if (versionList.isTruncated()) {
                    versionList = s3.listNextBatchOfVersions(versionList);
                } else {
                    if (!keys.isEmpty()) {
                        bulkDelete(s3, keys);
                        println(String.format("%s: %d objects deleted under %s", Thread.currentThread().getName(), counter, prefix));
                    }
                    break;
                }
            }

        } catch (AmazonServiceException e) {
            // The call was transmitted successfully, but Amazon S3 couldn't process
            // it, so it returned an error response.
            e.printStackTrace();
        } catch (SdkClientException e) {
            // Amazon S3 couldn't be contacted for a response, or the client couldn't
            // parse the response from Amazon S3.
            e.printStackTrace();
        }
        return counter;
    }

    private int bulkDelete(AmazonS3 s3, List<KeyVersion> keys) {
        int numberKeys = keys.size();
        DeleteObjectsRequest request = new DeleteObjectsRequest(bucket);
        request.setKeys(keys);
        s3.deleteObjects(request);
        keys.clear();
        return numberKeys;
    }

}

