[![Build Status](https://travis-ci.com/globalbioticinteractions/maven-s3-wagon.svg?branch=master)](https://travis-ci.com/globalbioticinteractions/maven-s3-wagon)

This plugin is under development and has not yet been released. 

# Maven S3 Wagon

This wagon enables communication between Maven and Amazon S3.

pom's with a reference to this wagon can publish build artifacts (.jar's, .war's, etc) to S3.

This project is based on https://github.com/jcaddel/maven-s3-wagon/ and https://github.com/seahen/maven-s3-wagon 

It was forked to upgrade to the latest s3 sdk, reduce dependencies on no-longer maintained libraries (e.g., kuali-s3) and to help support non-aws endpoints.

# Documentation

## Usage

Add this to the build section of a pom:

```
    <build>
      <extensions>
        <extension>
          <groupId>org.globalbioticinteractions</groupId>
          <artifactId>maven-s3-wagon</artifactId>
          <version>[S3 Wagon Version]</version>
       </extension>
      </extensions>
    </build>
```

Add this to the distribution management section:

```
    <distributionManagement>
      <site>
        <id>s3.site</id>
        <url>s3://[S3 Bucket Name]/site</url>
      </site>
      <repository>
        <id>s3.release</id>
        <url>s3://[S3 Bucket Name]/release</url>
      </repository>
      <snapshotRepository>
        <id>s3.snapshot</id>
        <url>s3://[S3 Bucket Name]/snapshot</url>
      </snapshotRepository>
    </distributionManagement>
 ```

Add server entries in maven's ```settings.xml```


```
    <servers>
      <server>
        <id>[repository id]</id>
        <username>[AWS Access Key ID]</username>
        <password>[AWS Secret Access Key]</password>
      </server>
    </servers>
```
When using non-AWS s3 endpoints (e.g., using https://min.io), please add your own endpoint like:

```
    <servers>
      <server>
        <id>[repository id]</id>
        <username>[AWS Access Key ID]</username>
        <password>[AWS Secret Access Key]</password>
        <configuration>
          <endpoint>https://s3.example.org</endpoint>
        </configuration>
      </server>
    </servers>
```




And setup one of the supported authentication techniques (see below)

If things are setup correctly, `$ mvn deploy` will produce output similar to this:

    [INFO] --- maven-deploy-plugin:2.7:deploy (default-deploy) @ kuali-example ---
    [INFO] Logged in - maven.kuali.org
    Uploading: s3://maven.kuali.org/release/org/kuali/common/kuali-example/1.0.0/kuali-example-1.0.0.jar
    [INFO] Logged off - maven.kuali.org
    [INFO] Transfers: 1 Time: 2.921s Amount: 7.6M Throughput: 2.6 MB/s
    [INFO] ------------------------------------------------------------------------
    [INFO] BUILD SUCCESS
    [INFO] ------------------------------------------------------------------------
