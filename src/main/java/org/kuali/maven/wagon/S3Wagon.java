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
import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.internal.ResettableInputStream;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.internal.Mimetypes;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.maven.wagon.ResourceDoesNotExistException;
import org.apache.maven.wagon.TransferFailedException;
import org.apache.maven.wagon.authentication.AuthenticationInfo;
import org.apache.maven.wagon.proxy.ProxyInfo;
import org.apache.maven.wagon.repository.Repository;
import org.kuali.common.aws.s3.S3Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
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
public class S3Wagon extends AbstractWagon implements RequestFactory {

    /**
     * Set the system property <code>maven.wagon.protocol</code> to <code>http</code> to force the wagon to communicate over <code>http</code>. Default is <code>https</code>.
     */
    public static final int DEFAULT_READ_TIMEOUT = 60 * 1000;
    private static final File TEMP_DIR = getCanonicalFile(System.getProperty("java.io.tmpdir"));
    private static final String TEMP_DIR_PATH = TEMP_DIR.getAbsolutePath();

    private int readTimeout = DEFAULT_READ_TIMEOUT;
    private TransferManager transferManager;

    private static final Logger log = LoggerFactory.getLogger(S3Wagon.class);

    private AmazonS3Client client;

    public String getBucketName() {
        return bucketName;
    }

    public void setBucketName(String bucketName) {
        this.bucketName = bucketName;
    }

    public void setBaseDir(String baseDir) {
        this.baseDir = baseDir;
    }

    private String bucketName;
    private String baseDir;

    public String getEndpoint() {
        return endpoint;
    }

    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    private String endpoint = null;

    private final Mimetypes mimeTypes = Mimetypes.getInstance();

    public S3Wagon() {
        super(true);
        S3Listener listener = new S3Listener();
        super.addSessionListener(listener);
        super.addTransferListener(listener);
    }

    protected AmazonS3Client getAmazonS3Client(final AWSCredentials credentials) {
        AmazonS3ClientBuilder builder = AmazonS3ClientBuilder
                .standard()
                .withClientConfiguration(new ClientConfiguration())
                .withCredentials(new AWSStaticCredentialsProvider(credentials));

        builder = enableCustomEndpointIfNeeded(builder);

        return (AmazonS3Client) builder.build();
    }

    private AmazonS3ClientBuilder enableCustomEndpointIfNeeded(AmazonS3ClientBuilder builder) {
        log.debug("endpoint: [" + getEndpoint() + "]");
        if (StringUtils.isNotBlank(getEndpoint())) {
            builder = builder.withEndpointConfiguration(
                    new AwsClientBuilder.EndpointConfiguration(endpoint, null)
            ).enablePathStyleAccess();
            log.debug("enabled custom endpoint [" + endpoint + "]");
        }
        return builder;
    }

    @Override
    protected void connectToRepository(Repository source, AuthenticationInfo auth, ProxyInfo proxy) {

        if (auth == null
                || StringUtils.isBlank(auth.getPassword())
                || StringUtils.isBlank(auth.getUserName())) {
            throw new IllegalStateException("The S3 wagon needs AWS Access Key set as the username and AWS Secret Key set as the password. eg:\n" +
                    "<server>\n" +
                    "  <id>my.server</id>\n" +
                    "  <username>[AWS Access Key ID]</username>\n" +
                    "  <password>[AWS Secret Access Key]</password>\n" +
                    "</server>\n");
        }

        AWSCredentials credentials = new BasicAWSCredentials(
                auth.getUserName(),
                auth.getPassword()
        );

        this.client = getAmazonS3Client(credentials);
        this.transferManager = TransferManagerBuilder
                .standard()
                .withS3Client(this.client)
                .build();

        this.bucketName = source.getHost();
        this.baseDir = getRepositoryBaseDir(source);
    }

    @Override
    protected boolean doesRemoteResourceExist(final String resourceName) {
        try {
            client.getObjectMetadata(bucketName, getBaseDir() + resourceName);
        } catch (AmazonClientException e) {
            return false;
        }
        return true;
    }

    @Override
    protected void disconnectFromRepository() {
        // Nothing to do for S3
    }

    /**
     * Pull an object out of an S3 bucket and write it to a file
     */
    @Override
    protected void getResource(final String resourceName, final File destination, final TransferProgress progress) throws ResourceDoesNotExistException, IOException {
        // Obtain the object from S3
        S3Object object;
        try {
            String key = baseDir + resourceName;
            object = client.getObject(bucketName, key);
        } catch (Exception e) {
            throw new ResourceDoesNotExistException("Resource " + resourceName + " does not exist in the repository", e);
        }

        // first write the file to a temporary location
        File temporaryDestination = File.createTempFile(destination.getName(), ".tmp", destination.getParentFile());
        InputStream in = null;
        OutputStream out = null;
        try {
            in = object.getObjectContent();
            out = new TransferProgressFileOutputStream(temporaryDestination, progress);
            byte[] buffer = new byte[1024];
            int length;
            while ((length = in.read(buffer)) != -1) {
                out.write(buffer, 0, length);
            }
        } finally {
            IOUtils.closeQuietly(in);
            IOUtils.closeQuietly(out);
        }
        // then move, to have an atomic operation to guarantee we don't have a partially downloaded file on disk
        temporaryDestination.renameTo(destination);
    }

    /**
     * Is the S3 object newer than the timestamp passed in?
     */
    @Override
    protected boolean isRemoteResourceNewer(final String resourceName, final long timestamp) {
        ObjectMetadata metadata = client.getObjectMetadata(bucketName, baseDir + resourceName);
        return metadata.getLastModified().compareTo(new Date(timestamp)) < 0;
    }

    /**
     * List all of the objects in a given directory
     */
    @Override
    protected List<String> listDirectory(String directory) throws Exception {
        // info("directory=" + directory);
        if (StringUtils.isBlank(directory)) {
            directory = "";
        }
        String delimiter = "/";
        String prefix = baseDir + directory;
        if (!prefix.endsWith(delimiter)) {
            prefix += delimiter;
        }
        // info("prefix=" + prefix);
        ListObjectsRequest request = new ListObjectsRequest();
        request.setBucketName(bucketName);
        request.setPrefix(prefix);
        request.setDelimiter(delimiter);
        ObjectListing objectListing = client.listObjects(request);
        // info("truncated=" + objectListing.isTruncated());
        // info("prefix=" + prefix);
        // info("basedir=" + basedir);
        List<String> fileNames = new ArrayList<String>();
        for (S3ObjectSummary summary : objectListing.getObjectSummaries()) {
            // info("summary.getKey()=" + summary.getKey());
            String key = summary.getKey();
            String relativeKey = key.startsWith(baseDir) ? key.substring(baseDir.length()) : key;
            boolean add = !StringUtils.isBlank(relativeKey) && !relativeKey.equals(directory);
            if (add) {
                // info("Adding key - " + relativeKey);
                fileNames.add(relativeKey);
            }
        }
        for (String commonPrefix : objectListing.getCommonPrefixes()) {
            String value = commonPrefix.startsWith(baseDir) ? commonPrefix.substring(baseDir.length()) : commonPrefix;
            // info("commonPrefix=" + commonPrefix);
            // info("relativeValue=" + relativeValue);
            // info("Adding common prefix - " + value);
            fileNames.add(value);
        }
        // StringBuilder sb = new StringBuilder();
        // sb.append("\n");
        // for (String fileName : fileNames) {
        // sb.append(fileName + "\n");
        // }
        // info(sb.toString());
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
    protected String getCanonicalKey(String key) {
        // release/./css/style.css
        String path = baseDir + key;

        // /temp/release/css/style.css
        File file = getCanonicalFile(new File(TEMP_DIR, path));
        String canonical = file.getAbsolutePath();

        // release/css/style.css
        int pos = TEMP_DIR_PATH.length() + 1;
        String suffix = canonical.substring(pos);

        // Always replace backslash with forward slash just in case we are running on Windows
        String canonicalKey = suffix.replace("\\", "/");

        // Return the canonical key
        return canonicalKey;
    }

    protected static File getCanonicalFile(String path) {
        return getCanonicalFile(new File(path));
    }

    protected static File getCanonicalFile(File file) {
        try {
            return new File(file.getCanonicalPath());
        } catch (IOException e) {
            throw new IllegalArgumentException("Unexpected IO error", e);
        }
    }

    protected ObjectMetadata getObjectMetadata(final File source, final String destination) {
        // Set the mime type according to the extension of the destination file
        String contentType = mimeTypes.getMimetype(destination);
        long contentLength = source.length();

        ObjectMetadata omd = new ObjectMetadata();
        omd.setContentLength(contentLength);
        omd.setContentType(contentType);
        return omd;
    }

    /**
     * Create a PutObjectRequest based on the PutContext
     */
    public PutObjectRequest getPutObjectRequest(PutFileContext context) {
        File source = context.getSource();
        String destination = context.getDestination();
        TransferProgress progress = context.getProgress();
        return getPutObjectRequest(source, destination, progress);
    }

    protected InputStream getInputStream(File source, TransferProgress progress) throws IOException {
        if (progress == null) {
            return new ResettableInputStream(source);
        } else {
            return new TransferProgressFileInputStream(source, progress);
        }
    }

    /**
     * Create a PutObjectRequest based on the source file and destination passed in.
     *
     * @param source      Local file to upload.
     * @param destination Destination S3 key.
     * @param progress    Transfer listener.
     * @return {@link PutObjectRequest} instance.
     */
    protected PutObjectRequest getPutObjectRequest(File source, String destination, TransferProgress progress) {
        try {
            String key = getCanonicalKey(destination);
            InputStream input = getInputStream(source, progress);
            ObjectMetadata metadata = getObjectMetadata(source, destination);
            return new PutObjectRequest(bucketName, key, input, metadata);
        } catch (FileNotFoundException e) {
            throw new AmazonServiceException("File not found", e);
        } catch (IOException e) {
            throw new AmazonServiceException("Error reading file", e);
        }
    }

    /**
     * On S3 there are no true "directories". An S3 bucket is essentially a Hashtable of files stored by key. The integration between a traditional file system and an S3 bucket is
     * to use the path of the file on the local file system as the key to the file in the bucket. The S3 bucket does not contain a separate key for the directory itself.
     */
    public final void putDirectory(File sourceDir, String destinationDir) throws TransferFailedException {

        // Examine the contents of the directory
        List<PutFileContext> contexts = getPutFileContexts(sourceDir, destinationDir);
        for (PutFileContext context : contexts) {
            PutObjectRequest request = getPutObjectRequest(context);
            // Upload the file to S3, using multi-part upload for large files
            S3Utils.getInstance().upload(context.getSource(), request, client, transferManager);
        }

    }

    /**
     * Store a resource into S3
     */
    @Override
    protected void putResource(final File source, final String destination, final TransferProgress progress) throws IOException {

        // Create a new PutObjectRequest
        PutObjectRequest request = getPutObjectRequest(source, destination, progress);

        // Upload the file to S3, using multi-part upload for large files
        S3Utils.getInstance().upload(source, request, client, transferManager);
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

    public String getBaseDir() {
        return baseDir;
    }

    @Override
    protected PutFileContext getPutFileContext(File source, String destination) {
        return super.getPutFileContext(source, destination);
    }

    public int getReadTimeout() {
        return readTimeout;
    }

    public void setReadTimeout(int readTimeout) {
        this.readTimeout = readTimeout;
    }

}
