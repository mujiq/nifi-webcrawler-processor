package com.example.nifi.processors.webcrawler;

import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.commons.lang3.StringUtils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManager;
import org.apache.hc.client5.http.socket.ConnectionSocketFactory;
import org.apache.hc.client5.http.socket.PlainConnectionSocketFactory;
import org.apache.hc.client5.http.ssl.SSLConnectionSocketFactory;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.config.Registry;
import org.apache.hc.core5.http.config.RegistryBuilder;
import org.apache.hc.core5.ssl.SSLContexts;

import javax.net.ssl.SSLContext;
import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

@Tags({"REST", "API", "Web", "Crawler", "HTTP", "HTTPS", "DFS"})
@CapabilityDescription("A processor that crawls REST APIs recursively using Depth-First Search (DFS) approach. " +
                       "Configurable to handle paginated APIs and follow linked resources.")
@WritesAttributes({
    @WritesAttribute(attribute = "webcrawler.url", description = "The URL of the API endpoint that was crawled"),
    @WritesAttribute(attribute = "webcrawler.depth", description = "The depth level of the crawled resource"),
    @WritesAttribute(attribute = "webcrawler.parent.url", description = "The parent URL that led to this resource"),
    @WritesAttribute(attribute = "webcrawler.content.type", description = "Content type of the response"),
    @WritesAttribute(attribute = "webcrawler.status.code", description = "HTTP status code of the response"),
    @WritesAttribute(attribute = "webcrawler.timestamp", description = "Timestamp when the resource was crawled")
})
@TriggerSerially
public class RESTAPIWebCrawler extends AbstractProcessor {

    // Property Descriptors
    public static final PropertyDescriptor BASE_URL = new PropertyDescriptor.Builder()
            .name("Base URL")
            .description("The base URL to start crawling from")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.URL_VALIDATOR)
            .build();
    
    public static final PropertyDescriptor MAX_DEPTH = new PropertyDescriptor.Builder()
            .name("Maximum Depth")
            .description("The maximum recursion depth for the crawler (0 means only crawl the base URL)")
            .required(true)
            .defaultValue("5")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();
    
    public static final PropertyDescriptor URL_PATTERN = new PropertyDescriptor.Builder()
            .name("URL Pattern")
            .description("Regex pattern to match URLs for recursion (leave empty to follow all links)")
            .required(false)
            .addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR)
            .build();
    
    public static final PropertyDescriptor PAGINATION_LINK_JSONPATH = new PropertyDescriptor.Builder()
            .name("Pagination Link JSONPath")
            .description("JSONPath expression to extract pagination link (e.g., '$.next' or '$.links.next')")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    
    public static final PropertyDescriptor RESOURCE_LINKS_JSONPATH = new PropertyDescriptor.Builder()
            .name("Resource Links JSONPath")
            .description("JSONPath expression to extract resource links (e.g., '$.items[*].href' or '$.links[*].url')")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    
    public static final PropertyDescriptor CONNECT_TIMEOUT = new PropertyDescriptor.Builder()
            .name("Connection Timeout")
            .description("Max wait time for connection to remote service (in milliseconds)")
            .required(true)
            .defaultValue("30000")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();
    
    public static final PropertyDescriptor READ_TIMEOUT = new PropertyDescriptor.Builder()
            .name("Read Timeout")
            .description("Max wait time for data from remote service (in milliseconds)")
            .required(true)
            .defaultValue("30000")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor SSL_CONTEXT_SERVICE = new PropertyDescriptor.Builder()
            .name("SSL Context Service")
            .description("The SSL Context Service used to provide client certificate information for TLS/SSL connections")
            .required(false)
            .identifiesControllerService(SSLContextService.class)
            .build();
    
    public static final PropertyDescriptor HEADERS = new PropertyDescriptor.Builder()
            .name("Request Headers")
            .description("Request headers as key-value pairs (format: header1:value1,header2:value2)")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
            
    public static final PropertyDescriptor AUTH_TYPE = new PropertyDescriptor.Builder()
            .name("Authentication Type")
            .description("The type of authentication to use")
            .required(false)
            .allowableValues("None", "Basic", "Bearer", "API Key")
            .defaultValue("None")
            .build();
    
    public static final PropertyDescriptor AUTH_USERNAME = new PropertyDescriptor.Builder()
            .name("Authentication Username")
            .description("Username for Basic Authentication")
            .required(false)
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    
    public static final PropertyDescriptor AUTH_PASSWORD = new PropertyDescriptor.Builder()
            .name("Authentication Password")
            .description("Password for Basic Authentication")
            .required(false)
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    
    public static final PropertyDescriptor AUTH_TOKEN = new PropertyDescriptor.Builder()
            .name("Authentication Token")
            .description("Token for Bearer Authentication or API Key")
            .required(false)
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    
    public static final PropertyDescriptor API_KEY_HEADER = new PropertyDescriptor.Builder()
            .name("API Key Header Name")
            .description("Header name for API Key Authentication")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("X-API-Key")
            .build();

    // Relationships
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Successful API responses are routed to this relationship")
            .build();
    
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Failed API requests are routed to this relationship")
            .build();

    private List<PropertyDescriptor> properties;
    private Set<Relationship> relationships;
    private final AtomicReference<ObjectMapper> objectMapperRef = new AtomicReference<>();
    private final ConcurrentHashMap<String, Boolean> visitedUrls = new ConcurrentHashMap<>();

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(BASE_URL);
        properties.add(MAX_DEPTH);
        properties.add(URL_PATTERN);
        properties.add(PAGINATION_LINK_JSONPATH);
        properties.add(RESOURCE_LINKS_JSONPATH);
        properties.add(CONNECT_TIMEOUT);
        properties.add(READ_TIMEOUT);
        properties.add(SSL_CONTEXT_SERVICE);
        properties.add(HEADERS);
        properties.add(AUTH_TYPE);
        properties.add(AUTH_USERNAME);
        properties.add(AUTH_PASSWORD);
        properties.add(AUTH_TOKEN);
        properties.add(API_KEY_HEADER);
        this.properties = Collections.unmodifiableList(properties);
        
        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
        
        objectMapperRef.set(new ObjectMapper());
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        visitedUrls.clear();
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            flowFile = session.create();
        }

        final String baseUrl = context.getProperty(BASE_URL).evaluateAttributeExpressions(flowFile).getValue();
        final int maxDepth = context.getProperty(MAX_DEPTH).asInteger();
        final String urlPatternStr = context.getProperty(URL_PATTERN).getValue();
        
        Pattern urlPattern = null;
        if (!StringUtils.isBlank(urlPatternStr)) {
            urlPattern = Pattern.compile(urlPatternStr);
        }

        // Create a stack for DFS crawling
        Deque<CrawlItem> stack = new ConcurrentLinkedDeque<>();
        stack.push(new CrawlItem(baseUrl, 0, null));
        
        try {
            CloseableHttpClient httpClient = createHttpClient(context);
            
            // Process the stack using DFS
            while (!stack.isEmpty()) {
                CrawlItem currentItem = stack.pop();
                String url = currentItem.getUrl();
                int depth = currentItem.getDepth();
                
                // Skip if already visited or beyond max depth
                if (visitedUrls.containsKey(url) || depth > maxDepth) {
                    continue;
                }
                
                visitedUrls.put(url, true);
                
                getLogger().debug("Crawling URL: {} at depth {}", new Object[] {url, depth});
                
                try {
                    // Make the HTTP request
                    HttpGet httpGet = new HttpGet(url);
                    
                    // Add headers
                    addHeaders(httpGet, context, flowFile);
                    
                    try (CloseableHttpResponse response = httpClient.execute(httpGet)) {
                        int statusCode = response.getCode();
                        HttpEntity entity = response.getEntity();
                        
                        if (statusCode >= 200 && statusCode < 300 && entity != null) {
                            // Read response content
                            String responseContent = EntityUtils.toString(entity);
                            
                            // Create a FlowFile for this response
                            FlowFile responseFlowFile = session.create(flowFile);
                            final Map<String, String> attributes = new HashMap<>();
                            attributes.put("webcrawler.url", url);
                            attributes.put("webcrawler.depth", String.valueOf(depth));
                            attributes.put("webcrawler.parent.url", currentItem.getParentUrl() != null ? currentItem.getParentUrl() : "");
                            attributes.put("webcrawler.content.type", response.getHeader("Content-Type") != null ? 
                                                                     response.getHeader("Content-Type").getValue() : "");
                            attributes.put("webcrawler.status.code", String.valueOf(statusCode));
                            attributes.put("webcrawler.timestamp", String.valueOf(System.currentTimeMillis()));
                            
                            responseFlowFile = session.putAllAttributes(responseFlowFile, attributes);
                            
                            // Write response content to FlowFile
                            responseFlowFile = session.write(responseFlowFile, out -> out.write(responseContent.getBytes()));
                            
                            // Transfer the FlowFile to success relationship
                            session.transfer(responseFlowFile, REL_SUCCESS);
                            
                            // If not at max depth, extract links and add to stack
                            if (depth < maxDepth) {
                                try {
                                    // Parse as JSON
                                    ObjectMapper mapper = objectMapperRef.get();
                                    JsonNode rootNode = mapper.readTree(responseContent);
                                    
                                    // Extract and process links
                                    List<String> extractedLinks = extractLinks(rootNode, context, url);
                                    
                                    for (String link : extractedLinks) {
                                        if (urlPattern == null || urlPattern.matcher(link).matches()) {
                                            stack.push(new CrawlItem(link, depth + 1, url));
                                        }
                                    }
                                } catch (Exception e) {
                                    getLogger().warn("Failed to parse JSON or extract links from {}: {}", new Object[]{url, e.getMessage()}, e);
                                }
                            }
                        } else {
                            // Handle failed request
                            FlowFile failureFlowFile = session.create(flowFile);
                            final Map<String, String> attributes = new HashMap<>();
                            attributes.put("webcrawler.url", url);
                            attributes.put("webcrawler.depth", String.valueOf(depth));
                            attributes.put("webcrawler.parent.url", currentItem.getParentUrl() != null ? currentItem.getParentUrl() : "");
                            attributes.put("webcrawler.status.code", String.valueOf(statusCode));
                            attributes.put("webcrawler.error.message", "HTTP Status: " + statusCode);
                            attributes.put("webcrawler.timestamp", String.valueOf(System.currentTimeMillis()));
                            
                            failureFlowFile = session.putAllAttributes(failureFlowFile, attributes);
                            session.transfer(failureFlowFile, REL_FAILURE);
                        }
                    }
                } catch (Exception e) {
                    getLogger().error("Error processing URL {}: {}", new Object[]{url, e.getMessage()}, e);
                    
                    // Create failure FlowFile
                    FlowFile failureFlowFile = session.create(flowFile);
                    final Map<String, String> attributes = new HashMap<>();
                    attributes.put("webcrawler.url", url);
                    attributes.put("webcrawler.depth", String.valueOf(depth));
                    attributes.put("webcrawler.parent.url", currentItem.getParentUrl() != null ? currentItem.getParentUrl() : "");
                    attributes.put("webcrawler.error.message", e.getMessage());
                    attributes.put("webcrawler.timestamp", String.valueOf(System.currentTimeMillis()));
                    
                    failureFlowFile = session.putAllAttributes(failureFlowFile, attributes);
                    session.transfer(failureFlowFile, REL_FAILURE);
                }
            }
            
            // Remove the original FlowFile if it was created here
            session.remove(flowFile);
            
        } catch (Exception e) {
            getLogger().error("Error in web crawler processor: {}", new Object[]{e.getMessage()}, e);
            flowFile = session.putAttribute(flowFile, "webcrawler.error.message", e.getMessage());
            session.transfer(flowFile, REL_FAILURE);
        }
    }

    private List<String> extractLinks(JsonNode rootNode, ProcessContext context, String baseUrl) {
        List<String> links = new ArrayList<>();
        
        // Extract pagination link if configured
        String paginationJsonPath = context.getProperty(PAGINATION_LINK_JSONPATH).getValue();
        if (!StringUtils.isBlank(paginationJsonPath)) {
            try {
                // Simple JSONPath-like extraction (basic implementation)
                String path = paginationJsonPath.replace("$.", "");
                String[] pathParts = path.split("\\.");
                
                JsonNode currentNode = rootNode;
                for (String part : pathParts) {
                    if (currentNode.has(part)) {
                        currentNode = currentNode.get(part);
                    } else {
                        currentNode = null;
                        break;
                    }
                }
                
                if (currentNode != null && currentNode.isTextual()) {
                    String nextPageUrl = currentNode.asText();
                    if (!StringUtils.isBlank(nextPageUrl)) {
                        links.add(resolveUrl(nextPageUrl, baseUrl));
                    }
                }
            } catch (Exception e) {
                getLogger().warn("Failed to extract pagination link using path {}: {}", 
                        new Object[]{paginationJsonPath, e.getMessage()}, e);
            }
        }
        
        // Extract resource links if configured
        String resourceLinksJsonPath = context.getProperty(RESOURCE_LINKS_JSONPATH).getValue();
        if (!StringUtils.isBlank(resourceLinksJsonPath)) {
            try {
                // Extract path and array notation if any
                String path = resourceLinksJsonPath.replace("$.", "");
                boolean isArrayPath = path.contains("[*]");
                
                if (isArrayPath) {
                    // Handle array notation (e.g., "items[*].href")
                    String[] pathBeforeArray = path.split("\\[\\*\\]")[0].split("\\.");
                    String[] pathAfterArray = path.substring(path.indexOf("[*]") + 3).split("\\.");
                    
                    // Navigate to the array
                    JsonNode arrayNode = rootNode;
                    for (String part : pathBeforeArray) {
                        if (arrayNode != null && arrayNode.has(part)) {
                            arrayNode = arrayNode.get(part);
                        } else {
                            arrayNode = null;
                            break;
                        }
                    }
                    
                    // Extract from each array element
                    if (arrayNode != null && arrayNode.isArray()) {
                        for (JsonNode item : arrayNode) {
                            JsonNode linkNode = item;
                            for (String part : pathAfterArray) {
                                if (part.startsWith(".")) part = part.substring(1);
                                if (!part.isEmpty() && linkNode != null && linkNode.has(part)) {
                                    linkNode = linkNode.get(part);
                                } else {
                                    linkNode = null;
                                    break;
                                }
                            }
                            
                            if (linkNode != null && linkNode.isTextual()) {
                                String link = linkNode.asText();
                                if (!StringUtils.isBlank(link)) {
                                    links.add(resolveUrl(link, baseUrl));
                                }
                            }
                        }
                    }
                } else {
                    // Handle simple path (direct property access)
                    String[] pathParts = path.split("\\.");
                    
                    JsonNode currentNode = rootNode;
                    for (String part : pathParts) {
                        if (currentNode != null && currentNode.has(part)) {
                            currentNode = currentNode.get(part);
                        } else {
                            currentNode = null;
                            break;
                        }
                    }
                    
                    if (currentNode != null) {
                        if (currentNode.isArray()) {
                            for (JsonNode item : currentNode) {
                                if (item.isTextual()) {
                                    String link = item.asText();
                                    if (!StringUtils.isBlank(link)) {
                                        links.add(resolveUrl(link, baseUrl));
                                    }
                                }
                            }
                        } else if (currentNode.isTextual()) {
                            String link = currentNode.asText();
                            if (!StringUtils.isBlank(link)) {
                                links.add(resolveUrl(link, baseUrl));
                            }
                        }
                    }
                }
            } catch (Exception e) {
                getLogger().warn("Failed to extract resource links using path {}: {}", 
                        new Object[]{resourceLinksJsonPath, e.getMessage()}, e);
            }
        }
        
        return links;
    }

    private String resolveUrl(String url, String baseUrl) {
        if (url.startsWith("http://") || url.startsWith("https://")) {
            return url; // Absolute URL
        } else if (url.startsWith("/")) {
            // Relative URL to domain root
            try {
                java.net.URL base = new java.net.URL(baseUrl);
                return base.getProtocol() + "://" + base.getHost() + 
                       (base.getPort() != -1 ? ":" + base.getPort() : "") + url;
            } catch (Exception e) {
                return url; // Return as is if parsing fails
            }
        } else {
            // Relative URL to current path
            if (baseUrl.endsWith("/")) {
                return baseUrl + url;
            } else {
                // Remove the last path segment
                int lastSlash = baseUrl.lastIndexOf('/');
                if (lastSlash > 8) { // After http(s)://
                    return baseUrl.substring(0, lastSlash + 1) + url;
                } else {
                    return baseUrl + "/" + url;
                }
            }
        }
    }

    private CloseableHttpClient createHttpClient(ProcessContext context) {
        SSLContextService sslContextService = context.getProperty(SSL_CONTEXT_SERVICE).asControllerService(SSLContextService.class);
        
        PoolingHttpClientConnectionManager connectionManager;
        
        if (sslContextService != null) {
            SSLContext sslContext = sslContextService.createSSLContext(SSLContextService.ClientAuth.REQUIRED);
            
            Registry<ConnectionSocketFactory> socketFactoryRegistry = RegistryBuilder.<ConnectionSocketFactory>create()
                .register("http", PlainConnectionSocketFactory.getSocketFactory())
                .register("https", new SSLConnectionSocketFactory(sslContext))
                .build();
            
            connectionManager = new PoolingHttpClientConnectionManager(socketFactoryRegistry);
        } else {
            connectionManager = new PoolingHttpClientConnectionManager();
        }
        
        connectionManager.setMaxTotal(10);
        connectionManager.setDefaultMaxPerRoute(10);
        
        return HttpClients.custom()
            .setConnectionManager(connectionManager)
            .disableAutomaticRetries()
            .build();
    }

    private void addHeaders(HttpGet httpGet, ProcessContext context, FlowFile flowFile) {
        // Add configured headers
        String headersValue = context.getProperty(HEADERS).getValue();
        if (!StringUtils.isBlank(headersValue)) {
            String[] headers = headersValue.split(",");
            for (String header : headers) {
                String[] parts = header.split(":", 2);
                if (parts.length == 2) {
                    httpGet.addHeader(parts[0].trim(), parts[1].trim());
                }
            }
        }
        
        // Add authentication headers based on auth type
        String authType = context.getProperty(AUTH_TYPE).getValue();
        if (!"None".equals(authType)) {
            switch (authType) {
                case "Basic":
                    String username = context.getProperty(AUTH_USERNAME).getValue();
                    String password = context.getProperty(AUTH_PASSWORD).getValue();
                    if (!StringUtils.isBlank(username) && !StringUtils.isBlank(password)) {
                        String auth = username + ":" + password;
                        String encodedAuth = Base64.getEncoder().encodeToString(auth.getBytes());
                        httpGet.addHeader("Authorization", "Basic " + encodedAuth);
                    }
                    break;
                    
                case "Bearer":
                    String token = context.getProperty(AUTH_TOKEN).getValue();
                    if (!StringUtils.isBlank(token)) {
                        httpGet.addHeader("Authorization", "Bearer " + token);
                    }
                    break;
                    
                case "API Key":
                    String apiKey = context.getProperty(AUTH_TOKEN).getValue();
                    String headerName = context.getProperty(API_KEY_HEADER).getValue();
                    if (!StringUtils.isBlank(apiKey) && !StringUtils.isBlank(headerName)) {
                        httpGet.addHeader(headerName, apiKey);
                    }
                    break;
            }
        }
        
        // Add common headers if not already set
        if (!httpGet.containsHeader("Accept")) {
            httpGet.addHeader("Accept", "application/json");
        }
        
        if (!httpGet.containsHeader("User-Agent")) {
            httpGet.addHeader("User-Agent", "Apache NiFi Web Crawler Processor");
        }
    }

    /**
     * Helper class to represent a crawl item in the stack
     */
    private static class CrawlItem {
        private final String url;
        private final int depth;
        private final String parentUrl;
        
        public CrawlItem(String url, int depth, String parentUrl) {
            this.url = url;
            this.depth = depth;
            this.parentUrl = parentUrl;
        }
        
        public String getUrl() {
            return url;
        }
        
        public int getDepth() {
            return depth;
        }
        
        public String getParentUrl() {
            return parentUrl;
        }
    }
} 