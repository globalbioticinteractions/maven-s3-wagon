package org.kuali.common.aws.s3;

import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class S3UtilsTest {

    @Test
    public void canonicalKey() {
        String key = S3Utils.getCanonicalKey("release/", "./css/style.css");
        assertThat(key, is("release/css/style.css"));
    }


}