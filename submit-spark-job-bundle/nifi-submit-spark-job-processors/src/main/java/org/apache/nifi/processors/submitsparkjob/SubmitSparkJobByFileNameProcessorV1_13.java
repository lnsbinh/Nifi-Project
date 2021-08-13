///*
// * Licensed to the Apache Software Foundation (ASF) under one or more
// * contributor license agreements.  See the NOTICE file distributed with
// * this work for additional information regarding copyright ownership.
// * The ASF licenses this file to You under the Apache License, Version 2.0
// * (the "License"); you may not use this file except in compliance with
// * the License.  You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//package org.apache.nifi.processors.submitsparkjob;
//
//import com.google.common.base.Strings;
//import okhttp3.Cache;
//import okhttp3.MediaType;
//import okhttp3.OkHttpClient;
//import okhttp3.Request;
//import okhttp3.RequestBody;
//import okhttp3.Response;
//import okhttp3.ResponseBody;
//import org.apache.commons.io.input.TeeInputStream;
//import org.apache.nifi.annotation.documentation.CapabilityDescription;
//import org.apache.nifi.components.PropertyDescriptor;
//import org.apache.nifi.expression.AttributeExpression;
//import org.apache.nifi.expression.ExpressionLanguageScope;
//import org.apache.nifi.flowfile.FlowFile;
//import org.apache.nifi.flowfile.attributes.CoreAttributes;
//import org.apache.nifi.logging.ComponentLog;
//import org.apache.nifi.processor.ProcessContext;
//import org.apache.nifi.processor.ProcessSession;
//import org.apache.nifi.processor.exception.ProcessException;
//import org.apache.nifi.processor.util.StandardValidators;
//import org.apache.nifi.processors.standard.InvokeHTTP;
//import org.apache.nifi.processors.standard.util.SoftLimitBoundedByteArrayOutputStream;
//import org.apache.nifi.stream.io.StreamUtils;
//
//import java.io.InputStream;
//import java.lang.reflect.Field;
//import java.lang.reflect.Method;
//import java.net.URL;
//import java.nio.charset.Charset;
//import java.util.Arrays;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//import java.util.UUID;
//import java.util.concurrent.TimeUnit;
//import java.util.concurrent.atomic.AtomicReference;
//import java.util.regex.Pattern;
//import java.util.stream.Collectors;
//import java.util.stream.Stream;
//
//import static org.apache.commons.lang3.StringUtils.trimToEmpty;
//
//@CapabilityDescription("An HTTP client processor which will interact with a configurable HTTP Endpoint when azure.filename attribute of incoming flowfile is SUCCESS_YYYY_MM_DD.json . The destination URL and HTTP Method are configurable. FlowFile attributes are converted to HTTP headers and the FlowFile contents are included as the body of the request (if the HTTP Method is PUT, POST or PATCH).")
//public class SubmitSparkJobByFileNameProcessorV1_13 extends InvokeHTTP {
//    public static final String AZURE_FILENAME_ATTRIBUTE = "azure.filename";
//    public static final PropertyDescriptor PROP_BODY;
//    public static final PropertyDescriptor PROP_HEADERS;
//    public static final List<PropertyDescriptor> NEW_PROPERTIES;
//
//    static {
//        PROP_BODY = (new PropertyDescriptor.Builder()).name("Body")
//                .description("Set Body value here")
//                .required(false)
//                .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(
//                        AttributeExpression.ResultType.STRING))
//                .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
//                .build();
//        PROP_HEADERS = (new PropertyDescriptor.Builder()).name("Headers")
//                .description("Rows are separated by new line. Keys and values are separated by : ")
//                .required(false)
//                .addValidator(
//                        StandardValidators
//                                .createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING))
//                .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
//                .build();
//        NEW_PROPERTIES = Stream.of(PROPERTIES, Arrays.asList(PROP_BODY, PROP_HEADERS)).flatMap(List::stream).collect(
//                Collectors.toList());
//    }
//
//    private final AtomicReference<OkHttpClient> refNewOkHttpClientAtomicReference;
//    private final Method refConfigureRequest;
//    private final Method refConvertAttributesFromHeaders;
//    private final Method refIsSuccess;
//    private final Method refGetCharsetFromMediaType;
//    private final Method refRoute;
//    private final Method refLogRequest;
//    private final Method refLogResponse;
//
//    public SubmitSparkJobByFileNameProcessorV1_13() {
//        super();
//
//        try {
//            Field fieldOkHttpClientAtomicReference = InvokeHTTP.class.getDeclaredField("okHttpClientAtomicReference");
//            fieldOkHttpClientAtomicReference.setAccessible(true);
//            refNewOkHttpClientAtomicReference = (AtomicReference<OkHttpClient>) fieldOkHttpClientAtomicReference
//                    .get(this);
//
//            refConfigureRequest = InvokeHTTP.class.getDeclaredMethod("configureRequest", ProcessContext.class,
//                    ProcessSession.class, FlowFile.class, URL.class);
//            refConfigureRequest.setAccessible(true);
//
//            refConvertAttributesFromHeaders = InvokeHTTP.class.getDeclaredMethod("convertAttributesFromHeaders",
//                    URL.class, Response.class);
//            refConvertAttributesFromHeaders.setAccessible(true);
//
//            refIsSuccess = InvokeHTTP.class.getDeclaredMethod("isSuccess", int.class);
//            refIsSuccess.setAccessible(true);
//
//            refGetCharsetFromMediaType = InvokeHTTP.class.getDeclaredMethod("getCharsetFromMediaType",
//                    MediaType.class);
//            refGetCharsetFromMediaType.setAccessible(true);
//
//            refRoute = InvokeHTTP.class.getDeclaredMethod("route", FlowFile.class, FlowFile.class,
//                    ProcessSession.class, ProcessContext.class, int.class);
//            refRoute.setAccessible(true);
//
//            refLogRequest = InvokeHTTP.class.getDeclaredMethod("logRequest", ComponentLog.class, Request.class);
//            refLogRequest.setAccessible(true);
//
//            refLogResponse = InvokeHTTP.class
//                    .getDeclaredMethod("logResponse", ComponentLog.class, URL.class, Response.class);
//            refLogResponse.setAccessible(true);
//        }
//        catch (Exception e) {
//            throw new RuntimeException(e);
//        }
//    }
//
//    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
//        return NEW_PROPERTIES;
//    }
//
//    public Request createHttpRequest(ProcessContext context, ProcessSession sesion, FlowFile requestFlowFile,
//            URL url) throws Exception {
//        try {
//            Request httpRequest = (Request) this.refConfigureRequest.invoke(this, context, sesion, requestFlowFile,
//                    url);
//            String body = context.getProperty(PROP_BODY).evaluateAttributeExpressions(requestFlowFile).getValue();
//            String headers = context.getProperty(PROP_HEADERS).evaluateAttributeExpressions(requestFlowFile).getValue();
//
//            if (Strings.isNullOrEmpty(body)) {
//                return httpRequest;
//            }
//
//            Request.Builder httpRequestBuilder = httpRequest.newBuilder();
//
//            if (!Strings.isNullOrEmpty(headers)) {
//                // Build header
//                String[] lines = headers.split("\\r?\\n|\\r");
//
//                for (String line : lines) {
//                    String[] arr = line.split(":", 2);
//
//                    // Replace value if exists
//                    httpRequestBuilder.header(arr[0], arr[1]);
//                }
//            }
//
//            httpRequestBuilder.method(httpRequest.method(),
//                    RequestBody.create(body, MediaType.get("application/json; charset=utf-8")));
//
//            return httpRequestBuilder.build();
//        }
//        catch (Exception e) {
//            if (e.getCause() instanceof IllegalArgumentException) {
//                throw new IllegalArgumentException(e.getCause());
//            }
//
//            throw e;
//        }
//    }
//
//    public boolean shouldTrigger(FlowFile requestFlowFile) {
//        String filename = requestFlowFile.getAttribute(AZURE_FILENAME_ATTRIBUTE);
//        Pattern valid = Pattern.compile(
//                "^SUCCESS_((19|2[0-9])[0-9]{2})_(0[1-9]|1[012])_(0[1-9]|[12][0-9]|3[01]).json$");
//
//        if (valid.matcher(filename).matches()) {
//            return true;
//        }
//
//        return false;
//    }
//
//    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
//        OkHttpClient okHttpClient = this.refNewOkHttpClientAtomicReference.get();
//
//        FlowFile requestFlowFile = session.get();
//
//        // Checking to see if the property to put the body of the response in an attribute was set
//        boolean putToAttribute = context.getProperty(PROP_PUT_OUTPUT_IN_ATTRIBUTE).isSet();
//        if (requestFlowFile == null) {
//            if (context.hasNonLoopConnection()) {
//                return;
//            }
//
//            String request = context.getProperty(PROP_METHOD).evaluateAttributeExpressions().getValue().toUpperCase();
//            if ("POST".equals(request) || "PUT".equals(request) || "PATCH".equals(request)) {
//                return;
//            }
//            else if (putToAttribute) {
//                requestFlowFile = session.create();
//            }
//        }
//
//        if (!shouldTrigger(requestFlowFile)) {
//            session.remove(requestFlowFile);
//            return;
//        }
//
//        // Setting some initial variables
//        final int maxAttributeSize = context.getProperty(PROP_PUT_ATTRIBUTE_MAX_LENGTH).asInteger();
//        final ComponentLog logger = getLogger();
//
//        // log ETag cache metrics
//        final boolean eTagEnabled = context.getProperty(PROP_USE_ETAG).asBoolean();
//        if (eTagEnabled && logger.isDebugEnabled()) {
//            final Cache cache = okHttpClient.cache();
//            logger.debug("OkHttp ETag cache metrics :: Request Count: {} | Network Count: {} | Hit Count: {}",
//                    new Object[] { cache.requestCount(), cache.networkCount(), cache.hitCount() });
//        }
//
//        // Every request/response cycle has a unique transaction id which will be stored as a flowfile attribute.
//        final UUID txId = UUID.randomUUID();
//
//        FlowFile responseFlowFile = null;
//        try {
//            // read the url property from the context
//            final String urlstr = trimToEmpty(
//                    context.getProperty(PROP_URL).evaluateAttributeExpressions(requestFlowFile).getValue());
//            final URL url = new URL(urlstr);
//
//            Request httpRequest = (Request) this.refConfigureRequest
//                    .invoke(this, context, session, requestFlowFile, url);
//
//            // log request
//            this.refLogRequest.invoke(this, logger, httpRequest);
//
//            // emit send provenance event if successfully sent to the server
//            if (httpRequest.body() != null) {
//                session.getProvenanceReporter().send(requestFlowFile, url.toExternalForm(), true);
//            }
//
//            final long startNanos = System.nanoTime();
//
//            try (Response responseHttp = okHttpClient.newCall(httpRequest).execute()) {
//                // output the raw response headers (DEBUG level only)
//                this.refLogResponse.invoke(this, logger, url, responseHttp);
//
//                // store the status code and message
//                int statusCode = responseHttp.code();
//                String statusMessage = responseHttp.message();
//
//                if (statusCode == 0) {
//                    throw new IllegalStateException("Status code unknown, connection hasn't been attempted.");
//                }
//
//                // Create a map of the status attributes that are always written to the request and response FlowFiles
//                Map<String, String> statusAttributes = new HashMap<>();
//                statusAttributes.put(STATUS_CODE, String.valueOf(statusCode));
//                statusAttributes.put(STATUS_MESSAGE, statusMessage);
//                statusAttributes.put(REQUEST_URL, url.toExternalForm());
//                statusAttributes.put(TRANSACTION_ID, txId.toString());
//
//                if (requestFlowFile != null) {
//                    requestFlowFile = session.putAllAttributes(requestFlowFile, statusAttributes);
//                }
//
//                // If the property to add the response headers to the request flowfile is true then add them
//                if (context.getProperty(PROP_ADD_HEADERS_TO_REQUEST).asBoolean() && requestFlowFile != null) {
//                    // write the response headers as attributes
//                    // this will overwrite any existing flowfile attributes
//                    requestFlowFile = session
//                            .putAllAttributes(requestFlowFile,
//                                    (Map<String, String>) this.refConvertAttributesFromHeaders
//                                            .invoke(url, responseHttp));
//                }
//
//                boolean outputBodyToRequestAttribute =
//                        (!(boolean) this.refIsSuccess.invoke(statusCode) || putToAttribute) && requestFlowFile != null;
//                boolean outputBodyToResponseContent =
//                        ((boolean) this.refIsSuccess.invoke(statusCode) && !putToAttribute) || context
//                                .getProperty(PROP_OUTPUT_RESPONSE_REGARDLESS).asBoolean();
//                ResponseBody responseBody = responseHttp.body();
//                boolean bodyExists = responseBody != null && !context.getProperty(IGNORE_RESPONSE_CONTENT).asBoolean();
//
//                InputStream responseBodyStream = null;
//                SoftLimitBoundedByteArrayOutputStream outputStreamToRequestAttribute = null;
//                TeeInputStream teeInputStream = null;
//                try {
//                    responseBodyStream = bodyExists ? responseBody.byteStream() : null;
//                    if (responseBodyStream != null && outputBodyToRequestAttribute && outputBodyToResponseContent) {
//                        outputStreamToRequestAttribute = new SoftLimitBoundedByteArrayOutputStream(maxAttributeSize);
//                        teeInputStream = new TeeInputStream(responseBodyStream, outputStreamToRequestAttribute);
//                    }
//
//                    if (outputBodyToResponseContent) {
//                        /*
//                         * If successful and putting to response flowfile, store the response body as the flowfile payload
//                         * we include additional flowfile attributes including the response headers and the status codes.
//                         */
//
//                        // clone the flowfile to capture the response
//                        if (requestFlowFile != null) {
//                            responseFlowFile = session.create(requestFlowFile);
//                        }
//                        else {
//                            responseFlowFile = session.create();
//                        }
//
//                        // write attributes to response flowfile
//                        responseFlowFile = session.putAllAttributes(responseFlowFile, statusAttributes);
//
//                        // write the response headers as attributes
//                        // this will overwrite any existing flowfile attributes
//                        responseFlowFile = session
//                                .putAllAttributes(responseFlowFile,
//                                        (Map<String, String>) this.refConvertAttributesFromHeaders
//                                                .invoke(url, responseHttp));
//
//                        // transfer the message body to the payload
//                        // can potentially be null in edge cases
//                        if (bodyExists) {
//                            // write content type attribute to response flowfile if it is available
//                            if (responseBody.contentType() != null) {
//                                responseFlowFile = session
//                                        .putAttribute(responseFlowFile, CoreAttributes.MIME_TYPE.key(),
//                                                responseBody.contentType().toString());
//                            }
//                            if (teeInputStream != null) {
//                                responseFlowFile = session.importFrom(teeInputStream, responseFlowFile);
//                            }
//                            else {
//                                responseFlowFile = session.importFrom(responseBodyStream, responseFlowFile);
//                            }
//
//                            // emit provenance event
//                            final long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
//                            if (requestFlowFile != null) {
//                                session.getProvenanceReporter().fetch(responseFlowFile, url.toExternalForm(), millis);
//                            }
//                            else {
//                                session.getProvenanceReporter().receive(responseFlowFile, url.toExternalForm(), millis);
//                            }
//                        }
//                    }
//
//                    // if not successful and request flowfile is not null, store the response body into a flowfile attribute
//                    if (outputBodyToRequestAttribute && bodyExists) {
//                        String attributeKey = context.getProperty(PROP_PUT_OUTPUT_IN_ATTRIBUTE)
//                                .evaluateAttributeExpressions(requestFlowFile).getValue();
//                        if (attributeKey == null) {
//                            attributeKey = RESPONSE_BODY;
//                        }
//                        byte[] outputBuffer;
//                        int size;
//
//                        if (outputStreamToRequestAttribute != null) {
//                            outputBuffer = outputStreamToRequestAttribute.getBuffer();
//                            size = outputStreamToRequestAttribute.size();
//                        }
//                        else {
//                            outputBuffer = new byte[maxAttributeSize];
//                            size = StreamUtils.fillBuffer(responseBodyStream, outputBuffer, false);
//                        }
//                        String bodyString = new String(outputBuffer, 0, size,
//                                (Charset) this.refGetCharsetFromMediaType.invoke(responseBody.contentType()));
//                        requestFlowFile = session.putAttribute(requestFlowFile, attributeKey, bodyString);
//
//                        final long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
//                        session.getProvenanceReporter().modifyAttributes(requestFlowFile, "The " + attributeKey
//                                + " has been added. The value of which is the body of a http call to "
//                                + url.toExternalForm() + ". It took " + millis + "millis,");
//                    }
//                }
//                finally {
//                    if (outputStreamToRequestAttribute != null) {
//                        outputStreamToRequestAttribute.close();
//                        outputStreamToRequestAttribute = null;
//                    }
//                    if (teeInputStream != null) {
//                        teeInputStream.close();
//                        teeInputStream = null;
//                    }
//                    else if (responseBodyStream != null) {
//                        responseBodyStream.close();
//                        responseBodyStream = null;
//                    }
//                }
//
//                this.refRoute.invoke(requestFlowFile, responseFlowFile, session, context, statusCode);
//
//            }
//        }
//        catch (final Exception e) {
//            // penalize or yield
//            if (requestFlowFile != null) {
//                logger.error("Routing to {} due to exception: {}", new Object[] { REL_FAILURE.getName(), e }, e);
//                requestFlowFile = session.penalize(requestFlowFile);
//                requestFlowFile = session.putAttribute(requestFlowFile, EXCEPTION_CLASS, e.getClass().getName());
//                requestFlowFile = session.putAttribute(requestFlowFile, EXCEPTION_MESSAGE, e.getMessage());
//                // transfer original to failure
//                session.transfer(requestFlowFile, REL_FAILURE);
//            }
//            else {
//                logger.error("Yielding processor due to exception encountered as a source processor: {}", e);
//                context.yield();
//            }
//
//            // cleanup response flowfile, if applicable
//            try {
//                if (responseFlowFile != null) {
//                    session.remove(responseFlowFile);
//                }
//            }
//            catch (final Exception e1) {
//                logger.error("Could not cleanup response flowfile due to exception: {}", new Object[] { e1 }, e1);
//            }
//        }
//    }
//}
