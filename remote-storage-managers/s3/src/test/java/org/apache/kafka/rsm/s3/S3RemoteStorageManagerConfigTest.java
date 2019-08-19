/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.rsm.s3;

import com.amazonaws.regions.Regions;
import org.apache.kafka.common.config.ConfigException;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

// TODO - check all expected exceptions with messages

public class S3RemoteStorageManagerConfigTest {
    @Test(expected = ConfigException.class)
    public void testEmptyConfig() {
        Map<String, String> props = new HashMap<>();
        new S3RemoteStorageManagerConfig(props);
    }

    @Test(expected = ConfigException.class)
    public void testEmptyS3BucketName() {
        Map<String, String> props = new HashMap<>();
        props.put("s3.bucket.name", "");
        S3RemoteStorageManagerConfig config = new S3RemoteStorageManagerConfig(props);
    }

    @Test
    public void testCorrectS3BucketName() {
        Map<String, String> props = new HashMap<>();
        props.put("s3.bucket.name", "test");
        S3RemoteStorageManagerConfig config = new S3RemoteStorageManagerConfig(props);
        assertEquals("test", config.s3BucketName());
    }

    @Test(expected = ConfigException.class)
    public void testIncorrectS3Region() {
        Map<String, String> props = new HashMap<>();
        props.put("s3.bucket.name", "test");
        props.put("s3.region", "eu-central-123");
        new S3RemoteStorageManagerConfig(props);
    }

    @Test
    public void testDefaultS3Region() {
        Map<String, String> props = new HashMap<>();
        props.put("s3.bucket.name", "test");
        S3RemoteStorageManagerConfig config = new S3RemoteStorageManagerConfig(props);
        assertEquals(Regions.DEFAULT_REGION, config.s3Region());
    }

    @Test
    public void testExplicitS3Region() {
        Map<String, String> props = new HashMap<>();
        props.put("s3.bucket.name", "test");
        props.put("s3.region", "eu-central-1");
        S3RemoteStorageManagerConfig config = new S3RemoteStorageManagerConfig(props);
        assertEquals(Regions.EU_CENTRAL_1, config.s3Region());
    }

    @Test(expected = ConfigException.class)
    public void testIncorrectAwsCredentialsProvider() {
        Map<String, String> props = new HashMap<>();
        props.put("s3.bucket.name", "test");
        props.put("s3.credentials.provider.class", "java.util.ArrayList");
        new S3RemoteStorageManagerConfig(props);
    }

    @Test(expected = ConfigException.class)
    public void testNonExistentAwsCredentialsProvider() {
        Map<String, String> props = new HashMap<>();
        props.put("s3.bucket.name", "test");
        props.put("s3.credentials.provider.class", "NonExistent");
        new S3RemoteStorageManagerConfig(props);
    }

    @Test
    public void testDefaultAwsCredentialsProvider() {
        Map<String, String> props = new HashMap<>();
        props.put("s3.bucket.name", "test");
        S3RemoteStorageManagerConfig config = new S3RemoteStorageManagerConfig(props);
        assertNull(config.awsCredentialsProvider());
    }

    @Test
    public void testExplicitAwsCredentialsProvider() {
        Map<String, String> props = new HashMap<>();
        props.put("s3.bucket.name", "test");
        props.put("s3.credentials.provider.class", "com.amazonaws.auth.SystemPropertiesCredentialsProvider");
        S3RemoteStorageManagerConfig config = new S3RemoteStorageManagerConfig(props);
        assertTrue(config.awsCredentialsProvider() instanceof com.amazonaws.auth.SystemPropertiesCredentialsProvider);
    }

    @Test(expected = ConfigException.class)
    public void testIncorrectIndexIntervalBytes() {
        Map<String, String> props = new HashMap<>();
        props.put("s3.bucket.name", "test");
        props.put("index.interval.bytes", "-1");
        new S3RemoteStorageManagerConfig(props);
    }

    @Test
    public void testDefaultIndexIntervalBytes() {
        Map<String, String> props = new HashMap<>();
        props.put("s3.bucket.name", "test");
        S3RemoteStorageManagerConfig config = new S3RemoteStorageManagerConfig(props);
        assertEquals(1024 * 1024, config.indexIntervalBytes());
    }

    @Test
    public void testExplicitIndexIntervalBytes() {
        Map<String, String> props = new HashMap<>();
        props.put("s3.bucket.name", "test");
        props.put("index.interval.bytes", "100");
        S3RemoteStorageManagerConfig config = new S3RemoteStorageManagerConfig(props);
        assertEquals(100, config.indexIntervalBytes());
    }
}
