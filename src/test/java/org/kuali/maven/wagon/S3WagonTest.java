/*
 * Copyright 2010-2015 The Kuali Foundation
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
package org.kuali.maven.wagon;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.apache.maven.wagon.ConnectionException;
import org.apache.maven.wagon.ResourceDoesNotExistException;
import org.apache.maven.wagon.TransferFailedException;
import org.apache.maven.wagon.Wagon;
import org.apache.maven.wagon.WagonException;
import org.apache.maven.wagon.authentication.AuthenticationException;
import org.apache.maven.wagon.authentication.AuthenticationInfo;
import org.apache.maven.wagon.authorization.AuthorizationException;
import org.apache.maven.wagon.repository.Repository;
import org.codehaus.plexus.util.FileUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;

public class S3WagonTest {

    private static final String testBucketName = "maven-s3-wagon-test-" + UUID.randomUUID();

    @BeforeClass
    public static void createTestBucket() {
        getClient().createBucket(testBucketName);
    }

    private static AmazonS3Client getClient() {
        return getS3WagonForMinioTestEndpoint()
                .getAmazonS3Client(getMinioTestAuth());
    }

    @AfterClass
    public static void cleanupAndRemoveTestBucket() {
        ObjectListing objectListing = getClient().listObjects(testBucketName);
        List<S3ObjectSummary> objectSummaries = objectListing.getObjectSummaries();
        for (S3ObjectSummary objectSummary : objectSummaries) {
            getClient().deleteObject(testBucketName, objectSummary.getKey());
        }
        getClient().deleteBucket(testBucketName);
    }


    @Test
    public void customEndpoint() throws ConnectionException, AuthenticationException {
        AuthenticationInfo auth = getMinioTestAuth();
        Repository repository = getTestRepo();
        S3Wagon wagon = getS3WagonForMinioTestEndpoint();
        wagon.connect(repository, auth);
    }

    private Repository getTestRepo() {
        return new Repository("minio.play", "s3://" + testBucketName + "/");
    }

    private static S3Wagon getS3WagonForMinioTestEndpoint() {
        S3Wagon wagon = new S3Wagon();
        wagon.setEndpoint("play.min.io");
        return wagon;
    }

    @Test
    public void putGetList() throws WagonException, URISyntaxException {
        AuthenticationInfo auth = getMinioTestAuth();
        Repository repository = getTestRepo();
        Properties parameters = new Properties();
        repository.setParameters(parameters);
        S3Wagon wagon = new S3Wagon();
        wagon.setEndpoint("play.min.io");
        wagon.connect(repository, auth);
        URL resource = getClass().getResource("/empty.properties");
        UUID uploadFilename = UUID.randomUUID();
        String uploadPath = "/integration-test/" + uploadFilename.toString();
        String localDownloadPath = "target/" + uploadFilename;
        File destination = new File(localDownloadPath);
        try {
            wagon.get(uploadPath, destination);
            fail("expected exception for not-yet-created file");
        } catch (ResourceDoesNotExistException ex) {
            //
        } finally {
            assertThat(destination.exists(), is(false));
        }

        wagon.put(new File(resource.toURI()), uploadPath);

        wagon.get(uploadPath, new File(localDownloadPath));
        assertThat(destination.exists(), is(true));

        List<String> fileList = wagon.getFileList("/integration-test");
        assertThat(fileList, hasItem(uploadPath));

    }

    private static AuthenticationInfo getMinioTestAuth() {
        AuthenticationInfo auth = new AuthenticationInfo();
        auth.setUserName("Q3AM3UQ867SPQQA43P2F");
        auth.setPassword("zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG");
        return auth;
    }

}
