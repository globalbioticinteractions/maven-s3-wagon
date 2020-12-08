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

import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility methods related to Amazon S3
 */
public class S3Utils {

    /**
     * Use aws sdk to manage multipart uploads.
     */
    public static void upload(PutObjectRequest request, TransferManager manager) {
        ObjectMetadata metadata = request.getMetadata();
        Upload upload = manager.upload(
                request.getBucketName(),
                request.getKey(),
                request.getInputStream(),
                metadata);
        try {
            upload.waitForCompletion();
        } catch (Exception e) {
            throw new AmazonS3Exception("Unexpected error uploading file", e);
        }
    }


}
