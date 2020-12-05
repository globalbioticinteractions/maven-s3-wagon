/**
 * Copyright 2010-2012 The Kuali Foundation
 *
 * Licensed under the Educational Community License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.opensource.org/licenses/ecl2.php
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.kuali.common.aws.s3;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * Utility methods related to Amazon S3
 */
public class S3Utils {
    private static final Logger log = LoggerFactory.getLogger(S3Utils.class);
    // Use multi part upload for files larger than 100 megabytes
    public static final long MULTI_PART_UPLOAD_THRESHOLD = Size.MB.getValue() * 100;
    private static final String PREFIX = "prefix";
    private static final String COUNT = "count";
    private static final String SIZE = "size";

    private static S3Utils instance;

    public static synchronized S3Utils getInstance() {
        if (instance == null) {
            instance = new S3Utils();
        }
        return instance;
    }

    protected S3Utils() {
        super();
    }

    public AWSCredentials getCredentials(String accessKey, String secretKey) {
        return new BasicAWSCredentials(accessKey, secretKey);
    }

    public AmazonS3Client getClient(String accessKey, String secretKey) {
        AWSCredentials credentials = getCredentials(accessKey, secretKey);
        return new AmazonS3Client(credentials);
    }

    /**
     * Upload a single file to Amazon S3. If the file is larger than <code>MULTI_PART_UPLOAD_THRESHOLD</code> a multi-part upload is used.
     * Multi-part uploads split the file into several smaller chunks with each chunk being uploaded in a different thread. Once all the
     * threads have completed the file is automatically reassembled on S3 as a single file.
     */
    public void upload(File file, PutObjectRequest request, AmazonS3Client client, TransferManager manager) {
        // Store the file on S3
        if (file.length() < MULTI_PART_UPLOAD_THRESHOLD) {
            // Use normal upload for small files
            client.putObject(request);
        } else {
            log.debug("Blocking multi-part upload: " + file.getAbsolutePath());
            // Use multi-part upload for large files
            blockingMultiPartUpload(request, manager);
        }
    }

    /**
     * Use this method to reliably upload large files and wait until they are fully uploaded before continuing. Behind the scenes this is
     * accomplished by splitting the file up into manageable chunks and using separate threads to upload each chunk. Consider using
     * multi-part uploads on files larger than <code>MULTI_PART_UPLOAD_THRESHOLD</code>. When this method returns, all threads have finished
     * and the file has been reassembled on S3. The benefit to this method is that if any one thread fails, only the portion of the file
     * that particular thread was handling will have to be re-uploaded (instead of the entire file). A reasonable number of automatic
     * retries occurs if an individual upload thread fails before this method throws <code>AmazonS3Exception</code>
     */
    public void blockingMultiPartUpload(PutObjectRequest request, TransferManager manager) {
        // Use multi-part upload for large files
        Upload upload = manager.upload(request);
        try {
            // Block and wait for the upload to finish
            upload.waitForCompletion();
        } catch (Exception e) {
            throw new AmazonS3Exception("Unexpected error uploading file", e);
        }
    }


}
