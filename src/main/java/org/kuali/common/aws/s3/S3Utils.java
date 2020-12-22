/**
 * Copyright 2010-2012 The Kuali Foundation
 * <p>
 * Licensed under the Educational Community License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.opensource.org/licenses/ecl2.php
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.kuali.common.aws.s3;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.transfer.TransferManager;
import org.apache.maven.wagon.ResourceDoesNotExistException;
import org.apache.maven.wagon.TransferFailedException;
import org.apache.maven.wagon.WagonException;
import org.apache.maven.wagon.repository.Repository;

import java.io.File;
import java.io.IOException;

/**
 * Utility methods related to Amazon S3
 */
public class S3Utils {

    private static final File TEMP_DIR = getCanonicalFile(System.getProperty("java.io.tmpdir"));
    private static final String TEMP_DIR_PATH = TEMP_DIR.getAbsolutePath();

    public static void download(GetObjectRequest request,
                                TransferManager manager,
                                File destFile)
            throws WagonException {

        try {
            manager
                    .download(request, destFile)
                    .waitForCompletion();
        } catch (AmazonClientException | InterruptedException ex) {
            String resourceURI = getS3URI(request.getBucketName(), request.getKey());
            if (ex instanceof AmazonS3Exception) {
                if (404 == ((AmazonS3Exception) ex).getStatusCode()) {
                    throw new ResourceDoesNotExistException("requested non-existing resource [" + resourceURI + "]", ex);
                }
            }
            throw new TransferFailedException("failed to access [" + resourceURI + "]", ex);
        }
    }

    public static void upload(PutObjectRequest request, TransferManager manager) throws TransferFailedException {

        try {
            manager
                    .upload(request)
                    .waitForCompletion();
        } catch (Exception e) {
            throw new TransferFailedException("Unexpected error uploading file", e);
        }
    }

    public static String getS3URI(String bucketName, String key) {
        return "s3://" + bucketName + "/" + key;
    }

    /**
     * Convert "/" -&gt; ""<br>
     * Convert "/snapshot/" &gt; "snapshot/"<br>
     * Convert "/snapshot" -&gt; "snapshot/"<br>
     *
     * @param source Repository info.
     * @return Normalized repository base dir.
     */
    public static String getRepositoryBaseDir(final Repository source) {
        StringBuilder sb = new StringBuilder(source.getBasedir());
        sb.deleteCharAt(0);
        if (sb.length() == 0) {
            return "";
        }
        if (sb.charAt(sb.length() - 1) != '/') {
            sb.append('/');
        }
        return sb.toString();
    }

    public static File getCanonicalFile(String path) {
        return getCanonicalFile(new File(path));
    }

    public static File getCanonicalFile(File file) {
        try {
            return new File(file.getCanonicalPath());
        } catch (IOException e) {
            throw new IllegalArgumentException("Unexpected IO error", e);
        }
    }

    public static String getKey(String baseDir, String resourceName) {
        return baseDir + resourceName;
    }

    /**
     * Normalize the key to our S3 object:<br>
     * Convert <code>./css/style.css</code> into <code>/css/style.css</code><br>
     * Convert <code>/foo/bar/../../css/style.css</code> into <code>/css/style.css</code><br>
     *
     * @param baseDir path prefix
     * @param key S3 Key string.
     * @return Normalized version of {@code key}.
     */
    public static String getCanonicalKey(String baseDir, String key) {
        // release/./css/style.css
        String path = getKey(baseDir, key);

        // /temp/release/css/style.css
        File file = getCanonicalFile(new File(TEMP_DIR, path));
        String canonical = file.getAbsolutePath();

        // release/css/style.css
        int pos = TEMP_DIR_PATH.length() + 1;
        String suffix = canonical.substring(pos);

        // Always replace backslash with forward slash just in case we are running on Windows
        return suffix.replace("\\", "/");
    }
}
