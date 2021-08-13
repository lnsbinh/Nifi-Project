/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processors.submitsparkjob;

import com.google.common.base.Strings;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.RecordedRequest;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.SubmitSparkJobByFileNameProcessor;
import org.apache.nifi.processors.standard.InvokeHTTP;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static java.net.HttpURLConnection.HTTP_OK;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class SubmitSparkJobByFileNameProcessorTest extends InvokeHTTPTest {
    private final String headerValue = "Authorization:Bearer abc\n"
            + "key2:abc\n"
            + "Key3:def";

    private final String bodyValue = ""
            + "{\n"
            + "\"job_id\": 58\n"
            + "}";

    private SubmitSparkJobByFileNameProcessor spySubmitSparkJobByFileNameProcessor;

    public InvokeHTTP getTargetClassInstance() {
        SubmitSparkJobByFileNameProcessor target = new SubmitSparkJobByFileNameProcessor();
        spySubmitSparkJobByFileNameProcessor = Mockito.spy(target);

        return spySubmitSparkJobByFileNameProcessor;
    }

    @Before
    public void setShouldTrigger() {
        Mockito.doReturn(true).when(spySubmitSparkJobByFileNameProcessor).shouldTrigger(
                ArgumentMatchers.nullable(FlowFile.class));
    }

    @Test
    public void testPostBody() throws InterruptedException {
        runner.setProperty(InvokeHTTP.PROP_METHOD, POST_METHOD);
        runner.setProperty(SubmitSparkJobByFileNameProcessor.PROP_HEADERS, headerValue);
        runner.setProperty(SubmitSparkJobByFileNameProcessor.PROP_BODY, bodyValue);

        setUrlProperty();
        mockWebServer.enqueue(new MockResponse().setResponseCode(HTTP_OK));
        runner.enqueue(FLOW_FILE_CONTENT);
        runner.run();

        assertResponseSuccessRelationships();
        assertRelationshipStatusCodeEquals(InvokeHTTP.REL_RESPONSE, HTTP_OK);

        final RecordedRequest request = takeRequestCompleted();
        final String contentType = request.getHeader(CONTENT_TYPE_HEADER);
        assertNotNull("Content Type not found", contentType);

        assertTrue("Header did not exist Authorization", !Strings.isNullOrEmpty(request.getHeader("Authorization")));
        assertTrue("Header Authorization value wrong", "Bearer abc".equals(request.getHeader("Authorization")));

        assertTrue("Header did not exist key2", !Strings.isNullOrEmpty(request.getHeader("key2")));
        assertTrue("Header key2 value wrong", "abc".equals(request.getHeader("key2")));
        assertTrue("Header key3 value wrong", "def".equals(request.getHeader("key3")));

        final String actualBody = request.getBody().readUtf8();
        assertTrue("Boda Data not found", actualBody.equals(bodyValue));
    }

    @Test
    public void testPostBodyDidTrigger() throws InterruptedException {
        String azureFileNameNotTrigger = "SUCCESS_2021_08_12.json";

        runner.setProperty(InvokeHTTP.PROP_METHOD, POST_METHOD);
        runner.setProperty(SubmitSparkJobByFileNameProcessor.PROP_HEADERS, headerValue);
        runner.setProperty(SubmitSparkJobByFileNameProcessor.PROP_BODY, bodyValue);

        final Map<String, String> attributes = new HashMap<>();
        attributes.put(SubmitSparkJobByFileNameProcessor.AZURE_FILENAME_ATTRIBUTE,
                azureFileNameNotTrigger);

        setUrlProperty();
        mockWebServer.enqueue(new MockResponse().setResponseCode(HTTP_OK));
        runner.enqueue(FLOW_FILE_CONTENT, attributes);
        runner.run();

        assertResponseSuccessRelationships();
        assertRelationshipStatusCodeEquals(InvokeHTTP.REL_RESPONSE, HTTP_OK);

        final RecordedRequest request = takeRequestCompleted();
        final String contentType = request.getHeader(CONTENT_TYPE_HEADER);
        assertNotNull("Content Type not found", contentType);

        assertTrue("Header did not exist Authorization", !Strings.isNullOrEmpty(request.getHeader("Authorization")));
        assertTrue("Header Authorization value wrong", "Bearer abc".equals(request.getHeader("Authorization")));

        assertTrue("Header did not exist key2", !Strings.isNullOrEmpty(request.getHeader("key2")));
        assertTrue("Header key2 value wrong", "abc".equals(request.getHeader("key2")));
        assertTrue("Header key3 value wrong", "def".equals(request.getHeader("key3")));

        final String actualBody = request.getBody().readUtf8();
        assertTrue("Boda Data not found", actualBody.equals(bodyValue));
    }

    @Test
    public void testPostBodyNotTrigger() throws InterruptedException {
        // Reset spy shouldTrigger to call real method
        Mockito.doCallRealMethod()
                .when(spySubmitSparkJobByFileNameProcessor).shouldTrigger(ArgumentMatchers.any(FlowFile.class));

        runner.setProperty(InvokeHTTP.PROP_METHOD, POST_METHOD);
        runner.setProperty(SubmitSparkJobByFileNameProcessor.PROP_HEADERS, headerValue);
        runner.setProperty(SubmitSparkJobByFileNameProcessor.PROP_BODY, bodyValue);

        String azureFileNameNotTrigger = "foobar";
        final Map<String, String> attributes = new HashMap<>();
        attributes.put(SubmitSparkJobByFileNameProcessor.AZURE_FILENAME_ATTRIBUTE,
                azureFileNameNotTrigger);

        setUrlProperty();
        mockWebServer.enqueue(new MockResponse().setResponseCode(HTTP_OK));
        runner.enqueue(FLOW_FILE_CONTENT, attributes);
        runner.run();

        //        assertRelationshipStatusCodeEquals(InvokeHTTP.REL_RESPONSE, HTTP_OK);

        final RecordedRequest request = mockWebServer.takeRequest(TAKE_REQUEST_COMPLETED_TIMEOUT, TimeUnit.SECONDS);
        assertTrue("Request is executed", request == null);
        assertTrue("Not expected tranfer to next processor",
                !runner.getFlowFilesForRelationship(InvokeHTTP.REL_FAILURE).iterator().hasNext());
        assertTrue("Not expected tranfer to next processor",
                !runner.getFlowFilesForRelationship(InvokeHTTP.REL_SUCCESS_REQ).iterator().hasNext());
    }
}
