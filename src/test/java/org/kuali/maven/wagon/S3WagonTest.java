/**
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

    @Test
    public void customEndpoint() throws ConnectionException, AuthenticationException {
        AuthenticationInfo auth = new AuthenticationInfo();
        auth.setUserName("Q3AM3UQ867SPQQA43P2F");
        auth.setPassword("zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG");
        Repository repository = new Repository("minio.play", "s3://globi/");
        S3Wagon wagon = new S3Wagon();
        wagon.setEndpoint("play.min.io");
        wagon.connect(repository, auth);
    }

    @Test
    public void putGetList() throws WagonException, URISyntaxException {
        AuthenticationInfo auth = new AuthenticationInfo();
        auth.setUserName("Q3AM3UQ867SPQQA43P2F");
        auth.setPassword("zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG");
        Repository repository = new Repository("minio.play", "s3://rkbucket/");
        Properties parameters = new Properties();
        repository.setParameters(parameters);
        Wagon wagon = new S3Wagon();
        ((S3Wagon) wagon).setEndpoint("play.min.io");
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

}
