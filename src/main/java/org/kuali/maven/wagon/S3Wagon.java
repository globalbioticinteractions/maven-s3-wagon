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

import com.amazonaws.AmazonClientException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.maven.wagon.ConnectionException;
import org.apache.maven.wagon.ResourceDoesNotExistException;
import org.apache.maven.wagon.TransferFailedException;
import org.apache.maven.wagon.Wagon;
import org.apache.maven.wagon.authentication.AuthenticationException;
import org.apache.maven.wagon.authentication.AuthenticationInfo;
import org.apache.maven.wagon.authorization.AuthorizationException;
import org.apache.maven.wagon.events.SessionListener;
import org.apache.maven.wagon.events.TransferListener;
import org.apache.maven.wagon.proxy.ProxyInfo;
import org.apache.maven.wagon.proxy.ProxyInfoProvider;
import org.apache.maven.wagon.repository.Repository;
import org.kuali.common.aws.s3.S3Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * <p>
 * An implementation of the Maven Wagon interface that is integrated with the Amazon S3 service.
 * </p>
 *
 * <p>
 * URLs that reference the S3 service should be in the form of <code>s3://bucket.name</code>. As an example <code>s3://maven.kuali.org</code> puts files into the
 * <code>maven.kuali.org</code> bucket on the S3 service.
 * </p>
 *
 * <p>
 * This implementation uses the <code>username</code> and <code>password</code> portions of the server authentication metadata for credentials.
 * </p>
 *
 * @author Ben Hale
 * @author Jeff Caddel
 */
public class S3Wagon implements Wagon {

    private static final int DEFAULT_READ_TIMEOUT = 60 * 1000;
    private static final File TEMP_DIR = S3Utils.getCanonicalFile(System.getProperty("java.io.tmpdir"));
    private static final String TEMP_DIR_PATH = TEMP_DIR.getAbsolutePath();

    private int readTimeout = DEFAULT_READ_TIMEOUT;

    private TransferManager transferManager;

    private static final Logger log = LoggerFactory.getLogger(S3Wagon.class);

    private AmazonS3Client client;

    private String bucketName;
    private String baseDir;
    private String endpoint = null;

    private int timeout;

    private boolean interactive;

    private Repository repository;

    AmazonS3Client getAmazonS3Client(final AuthenticationInfo credentials) {
        AmazonS3ClientBuilder builder = AmazonS3ClientBuilder
                .standard()
                .withClientConfiguration(new ClientConfiguration())
                .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(
                        credentials.getUserName(),
                        credentials.getPassword()
                )));
        builder = enableCustomEndpointIfNeeded(builder);
        return (AmazonS3Client) builder.build();
    }

    private AmazonS3ClientBuilder enableCustomEndpointIfNeeded(AmazonS3ClientBuilder builder) {
        if (StringUtils.isNotBlank(getEndpoint())) {
            builder = builder.withEndpointConfiguration(
                    new AwsClientBuilder.EndpointConfiguration(endpoint, null)
            ).enablePathStyleAccess();
            log.info("using s3 endpoint: [" + endpoint + "]");
        }
        return builder;
    }

    @Override
    public final Repository getRepository() {
        return repository;
    }

    @Override
    public final boolean isInteractive() {
        return interactive;
    }

    @Override
    public final void setInteractive(final boolean interactive) {
        this.interactive = interactive;
    }

    @Override
    public final void disconnect() throws ConnectionException {
        try {
            if (getTransferManager() != null) {
                getTransferManager().shutdownNow();
            }
        } catch (Exception e) {
            throw new ConnectionException("Could not disconnect from repository", e);
        }
    }

    @Override
    public final void get(final String resourceName, final File destination) throws TransferFailedException, ResourceDoesNotExistException, AuthorizationException {
        try {
            File temporaryDestination = createTmpFile(destination);

            log.info("downloading: [" + S3Utils.getS3URI(getBucketName(), getKey(resourceName)) + "]...");
            S3Utils.download(new GetObjectRequest(getBucketName(), getKey(resourceName)),
                    getTransferManager(), temporaryDestination);

            try {
                // then move, to have an atomic operation to guarantee we don't have a partially downloaded file on disk
                FileUtils.moveFile(temporaryDestination, destination);
            } catch (IOException ex) {
                throw new TransferFailedException("failed to get [" + S3Utils.getS3URI(getBucketName(), getKey(resourceName)) + "]", ex);
            }
            log.info("downloaded: [" + S3Utils.getS3URI(getBucketName(), getKey(resourceName)) + "]...");
        } catch (TransferFailedException | AuthorizationException | ResourceDoesNotExistException e) {
            throw e;
        } catch (Exception e) {
            throw new TransferFailedException("Transfer of resource " + destination + "failed", e);
        }
    }

    public final List<String> getFileList(final String destinationDirectory) throws TransferFailedException, ResourceDoesNotExistException, AuthorizationException {
        try {
            return listDirectory(destinationDirectory);
        } catch (TransferFailedException | ResourceDoesNotExistException | AuthorizationException e) {
            throw e;
        } catch (Exception e) {
            throw new TransferFailedException("Listing of directory " + destinationDirectory + "failed", e);
        }
    }


    private long parseMultipartCopyPartSize(String multipartCopyPartSize) {
        try {
            return Long.parseLong(multipartCopyPartSize);
        } catch (NumberFormatException ex) {
            throw new IllegalStateException("The multipartCopyPartSize of S3 wagon needs to be a integer/long. eg:\n" +
                    "<server>\n" +
                    "  <id>my.server</id>\n" +
                    "  ...\n" +
                    "  <configuration>\n" +
                    "    <multipartCopyPartSize>10485760</multipartCopyPartSize>\n" +
                    "  </configuration>\n" +
                    "</server>\n", ex);
        }
    }

    private boolean doesRemoteResourceExist(final String resourceName) {
        try {
            client.getObjectMetadata(getBucketName(), getBaseDir() + resourceName);
        } catch (AmazonClientException e) {
            return false;
        }
        return true;
    }

    private File createTmpFile(File destination) throws TransferFailedException {
        try {
            return File.createTempFile(destination.getName(), ".tmp", destination.getParentFile());
        } catch (IOException e) {
            throw new TransferFailedException("cannot create temporary file for download", e);
        }
    }

    /**
     * Is the S3 object newer than the timestamp passed in?
     */
    private boolean isRemoteResourceNewer(final String resourceName, final long timestamp) {
        ObjectMetadata metadata = client.getObjectMetadata(getBucketName(), getKey(resourceName));
        return metadata.getLastModified().compareTo(new Date(timestamp)) < 0;
    }

    private String getKey(String resourceName) {
        return getBaseDir() + resourceName;
    }

    /**
     * List all of the objects in a given directory
     */
    private List<String> listDirectory(String directory) throws Exception {
        if (StringUtils.isBlank(directory)) {
            directory = "";
        }
        String delimiter = "/";
        String prefix = getKey(directory);
        if (!prefix.endsWith(delimiter)) {
            prefix += delimiter;
        }
        ListObjectsRequest request = new ListObjectsRequest();
        request.setBucketName(getBucketName());
        request.setPrefix(prefix);
        request.setDelimiter(delimiter);
        ObjectListing objectListing = client.listObjects(request);
        List<String> fileNames = new ArrayList<>();
        for (S3ObjectSummary summary : objectListing.getObjectSummaries()) {
            String key = summary.getKey();
            String relativeKey = key.startsWith(getBaseDir())
                    ? key.substring(getBaseDir().length())
                    : key;

            boolean add = !StringUtils.isBlank(relativeKey) && !relativeKey.equals(directory);
            if (add) {
                fileNames.add(relativeKey);
            }
        }
        for (String commonPrefix : objectListing.getCommonPrefixes()) {
            String value = commonPrefix.startsWith(getBaseDir())
                    ? commonPrefix.substring(getBaseDir().length())
                    : commonPrefix;
            fileNames.add(value);
        }
        return fileNames;
    }

    /**
     * Normalize the key to our S3 object:<br>
     * Convert <code>./css/style.css</code> into <code>/css/style.css</code><br>
     * Convert <code>/foo/bar/../../css/style.css</code> into <code>/css/style.css</code><br>
     *
     * @param key S3 Key string.
     * @return Normalized version of {@code key}.
     */
    private String getCanonicalKey(String key) {
        // release/./css/style.css
        String path = getKey(key);

        // /temp/release/css/style.css
        File file = S3Utils.getCanonicalFile(new File(TEMP_DIR, path));
        String canonical = file.getAbsolutePath();

        // release/css/style.css
        int pos = TEMP_DIR_PATH.length() + 1;
        String suffix = canonical.substring(pos);

        // Always replace backslash with forward slash just in case we are running on Windows
        return suffix.replace("\\", "/");
    }

    /**
     * Create a PutObjectRequest based on the PutContext
     */
    public PutObjectRequest createPutObjectRequest(PutFileContext context) {
        File source = context.getSource();
        String destination = context.getDestination();
        return createPutObjectRequest(source, destination);
    }

    /**
     * Create a PutObjectRequest based on the source file and destination passed in.
     *
     * @param source      Local file to upload.
     * @param destination Destination S3 key.
     * @return {@link PutObjectRequest} instance.
     */
    PutObjectRequest createPutObjectRequest(File source, String destination) {
        String key = getCanonicalKey(destination);
        return new PutObjectRequest(getBucketName(), key, source);
    }

    /**
     * On S3 there are no true "directories". An S3 bucket is essentially a Hashtable of files stored by key. The integration between a traditional file system and an S3 bucket is
     * to use the path of the file on the local file system as the key to the file in the bucket. The S3 bucket does not contain a separate key for the directory itself.
     */
    public final void putDirectory(File sourceDir, String destinationDir) throws TransferFailedException {
        List<PutFileContext> contexts = S3Utils.createPutFileContexts(sourceDir, destinationDir);
        for (PutFileContext context : contexts) {
            PutObjectRequest request = createPutObjectRequest(context);
            S3Utils.upload(request, getTransferManager());
        }
    }

    @Override
    public boolean supportsDirectoryCopy() {
        return true;
    }

    private String getBaseDir() {
        return baseDir;
    }

    public int getReadTimeout() {
        return readTimeout;
    }

    @Override
    public boolean hasTransferListener(TransferListener listener) {
        return false;
    }

    public void connect(final Repository source, final AuthenticationInfo authenticationInfo, final ProxyInfoProvider proxyInfoProvider) throws ConnectionException,
            AuthenticationException {
        doConnect(source, authenticationInfo, null);
    }

    @Override
    public void connect(final Repository source, final ProxyInfoProvider proxyInfoProvider) throws ConnectionException, AuthenticationException {
        doConnect(source, null, null);
    }

    @Override
    public final void connect(final Repository source) throws ConnectionException, AuthenticationException {
        doConnect(source, null, null);
    }

    @Override
    public final void connect(final Repository source, final ProxyInfo proxyInfo) throws ConnectionException, AuthenticationException {
        connect(source, null, proxyInfo);
    }

    @Override
    public final void connect(final Repository source, final AuthenticationInfo authenticationInfo) throws ConnectionException, AuthenticationException {
        doConnect(source, authenticationInfo, null);
    }

    private void doConnect(final Repository source, final AuthenticationInfo authenticationInfo, final ProxyInfo proxyInfo) throws ConnectionException {
        repository = source;
        log.debug("Connecting to " + repository.getUrl());
        try {
            if (authenticationInfo == null
                    || StringUtils.isBlank(authenticationInfo.getPassword())
                    || StringUtils.isBlank(authenticationInfo.getUserName())) {
                throw new IllegalStateException("The S3 wagon needs AWS Access Key set as the username and AWS Secret Key set as the password. eg:\n" +
                        "<server>\n" +
                        "  <id>my.server</id>\n" +
                        "  <username>[AWS Access Key ID]</username>\n" +
                        "  <password>[AWS Secret Access Key]</password>\n" +
                        "</server>\n");
            }

            this.client = getAmazonS3Client(authenticationInfo);
            String multipartCopyPartSize = source.getParameter("multipartCopyPartSize");

            // reduce default copy part size to increase friendliness to cloudflare and nginx
            long multipartCopyPartSize1 = StringUtils.isBlank(multipartCopyPartSize)
                    ? 1024 * 1024 * 10L
                    : parseMultipartCopyPartSize(multipartCopyPartSize);

            setTransferManager(TransferManagerBuilder
                    .standard()
                    .withMultipartCopyPartSize(multipartCopyPartSize1)
                    .withS3Client(this.client)
                    .build());

            setBucketName(source.getHost());
            setBaseDir(S3Utils.getRepositoryBaseDir(source));
        } catch (Exception e) {
            throw new ConnectionException("Could not connect to repository", e);
        }
    }

    @Override
    public final void connect(final Repository source, final AuthenticationInfo authenticationInfo, final ProxyInfo proxyInfo) throws ConnectionException, AuthenticationException {
        doConnect(source, authenticationInfo, proxyInfo);
    }

    @Override
    public final boolean getIfNewer(final String resourceName, final File destination, final long timestamp) throws TransferFailedException, ResourceDoesNotExistException,
            AuthorizationException {
        try {
            if (isRemoteResourceNewer(resourceName, timestamp)) {
                get(resourceName, destination);
                return true;
            } else {
                return false;
            }
        } catch (TransferFailedException | ResourceDoesNotExistException | AuthorizationException e) {
            throw e;
        } catch (Exception e) {
            throw new TransferFailedException("Transfer of resource " + destination + "failed", e);
        }
    }

    @Deprecated
    public final void openConnection() throws ConnectionException, AuthenticationException {
        // Nothing to do here (never called by the wagon manager)
    }

    @Override
    public final void put(final File source, final String destination) throws TransferFailedException, ResourceDoesNotExistException, AuthorizationException {
        PutObjectRequest request = createPutObjectRequest(source, destination);
        log.info("uploading: [" + S3Utils.getS3URI(getBucketName(), request.getKey()));
        S3Utils.upload(request, getTransferManager());
        log.info("uploaded: [" + S3Utils.getS3URI(getBucketName(), request.getKey()));
    }

    @Override
    public final boolean resourceExists(final String resourceName) throws TransferFailedException, AuthorizationException {
        try {
            return doesRemoteResourceExist(resourceName);
        } catch (Exception e) {
            throw new TransferFailedException("Listing of resource " + resourceName + "failed", e);
        }
    }


    @Override
    public int getTimeout() {
        return this.timeout;
    }

    @Override
    public void setTimeout(final int timeoutValue) {
        this.timeout = timeoutValue;
    }

    @Override
    public void addSessionListener(SessionListener listener) {

    }

    @Override
    public void removeSessionListener(SessionListener listener) {

    }

    @Override
    public boolean hasSessionListener(SessionListener listener) {
        return false;
    }

    @Override
    public void addTransferListener(TransferListener listener) {

    }

    @Override
    public void removeTransferListener(TransferListener listener) {

    }


    public void setReadTimeout(int readTimeout) {
        this.readTimeout = readTimeout;
    }

    private TransferManager getTransferManager() {
        return transferManager;
    }

    private void setTransferManager(TransferManager transferManager) {
        this.transferManager = transferManager;
    }

    private String getBucketName() {
        return bucketName;
    }

    void setBucketName(String bucketName) {
        this.bucketName = bucketName;
    }

    void setBaseDir(String baseDir) {
        this.baseDir = baseDir;
    }

    private String getEndpoint() {
        return endpoint;
    }

    void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }


}
