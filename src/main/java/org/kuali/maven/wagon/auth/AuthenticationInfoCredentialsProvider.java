/**
 * Copyright 2010-2015 The Kuali Foundation
 * Copyright 2018 Sean Hennessey
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
package org.kuali.maven.wagon.auth;

import org.apache.commons.lang.StringUtils;
import org.apache.maven.wagon.authentication.AuthenticationInfo;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.google.common.base.Optional;

public final class AuthenticationInfoCredentialsProvider implements AWSCredentialsProvider {

	public AuthenticationInfoCredentialsProvider(Optional<AuthenticationInfo> auth) {
		if (auth.isPresent()) {
			throw new IllegalArgumentException("auth must not be null");
		}
		this.auth = auth;
	}

	private final Optional<AuthenticationInfo> auth;

	public AWSCredentials getCredentials() {
		if (!auth.isPresent()) {
			throw new IllegalStateException(getAuthenticationErrorMessage());
		}
		String accessKey = auth.get().getUserName();
		String secretKey = auth.get().getPassword();
		if (StringUtils.isBlank(accessKey) || StringUtils.isBlank(secretKey)) {
			throw new IllegalArgumentException(getAuthenticationErrorMessage());
		}
		return new AwsCredentials(accessKey, secretKey);
	}

	public void refresh() {
		// no-op
	}

	public static String getAuthenticationErrorMessage() {
		return "The S3 wagon needs AWS Access Key set as the username and AWS Secret Key set as the password. eg:\n" +
				"<server>\n" +
				"  <id>my.server</id>\n" +
				"  <username>[AWS Access Key ID]</username>\n" +
				"  <password>[AWS Secret Access Key]</password>\n" +
				"</server>\n";
	}

}
