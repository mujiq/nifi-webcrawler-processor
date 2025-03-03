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

// Add imports for HTML parsing
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

// Add imports for PDF parsing
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.text.PDFTextStripper;

import javax.net.ssl.SSLContext;
import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.nio.charset.StandardCharsets;

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
public class WebCrawler extends AbstractProcessor {

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

    // Add HTML link extraction property
    public static final PropertyDescriptor HTML_LINK_SELECTOR = new PropertyDescriptor.Builder()
            .name("HTML Link Selector")
            .description("CSS selector to extract links from HTML content (e.g., 'a[href]' for all anchor tags)")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("a[href]")
            .build();
            
    // Add property to enable extraction from specific MIME types
    public static final PropertyDescriptor SUPPORTED_MIME_TYPES = new PropertyDescriptor.Builder()
            .name("Supported MIME Types")
            .description("Comma-separated list of MIME types to process (e.g., 'application/json,text/html,application/pdf'). Leave empty to process all types.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
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
        properties.add(HTML_LINK_SELECTOR);
        properties.add(SUPPORTED_MIME_TYPES);
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
        // Clear the visited URLs when the processor is scheduled
        visitedUrls.clear();
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        // Get processor properties
        final String baseUrl = context.getProperty(BASE_URL).evaluateAttributeExpressions().getValue();
        final int maxDepth = context.getProperty(MAX_DEPTH).asInteger();
        final String urlPattern = context.getProperty(URL_PATTERN).getValue();
        final Pattern urlRegexPattern = StringUtils.isNotEmpty(urlPattern) ? Pattern.compile(urlPattern) : null;
        final String paginationLinkJsonPath = context.getProperty(PAGINATION_LINK_JSONPATH).getValue();
        final String resourceLinksJsonPath = context.getProperty(RESOURCE_LINKS_JSONPATH).getValue();
        final String htmlLinkSelector = context.getProperty(HTML_LINK_SELECTOR).getValue();
        final String supportedMimeTypes = context.getProperty(SUPPORTED_MIME_TYPES).getValue();
        
        // Set up a queue for DFS traversal
        ConcurrentLinkedDeque<CrawlItem> queue = new ConcurrentLinkedDeque<>();
        
        // Add the base URL to the queue with depth 0
        queue.add(new CrawlItem(baseUrl, 0, baseUrl));
        
        // Process items in the queue using DFS approach
        while (!queue.isEmpty()) {
            CrawlItem currentItem = queue.pollFirst(); // For DFS, we use pollFirst (LIFO)
            
            // Skip if we've already visited this URL or if we've exceeded the maximum depth
            if (visitedUrls.containsKey(currentItem.getUrl()) || currentItem.getDepth() > maxDepth) {
                continue;
            }
            
            // Mark as visited
            visitedUrls.put(currentItem.getUrl(), true);
            
            // Set up HTTP client
            CloseableHttpClient httpClient = null;
            CloseableHttpResponse response = null;
            
            try {
                // Create HTTP client with SSL context if needed
                SSLContextService sslContextService = context.getProperty(SSL_CONTEXT_SERVICE).asControllerService(SSLContextService.class);
                
                if (sslContextService != null) {
                    SSLContext sslContext = sslContextService.createSSLContext(SSLContextService.ClientAuth.REQUIRED);
                    
                    Registry<ConnectionSocketFactory> socketFactoryRegistry = RegistryBuilder.<ConnectionSocketFactory>create()
                            .register("http", PlainConnectionSocketFactory.getSocketFactory())
                            .register("https", new SSLConnectionSocketFactory(sslContext))
                            .build();
                    
                    PoolingHttpClientConnectionManager connManager = new PoolingHttpClientConnectionManager(socketFactoryRegistry);
                    
                    httpClient = HttpClients.custom()
                            .setConnectionManager(connManager)
                            .build();
                } else {
                    httpClient = HttpClients.createDefault();
                }
                
                // Create GET request
                HttpGet httpGet = new HttpGet(currentItem.getUrl());
                
                // Set timeouts
                int connectTimeout = context.getProperty(CONNECT_TIMEOUT).asInteger();
                int readTimeout = context.getProperty(READ_TIMEOUT).asInteger();
                
                // Add headers if specified
                String headersValue = context.getProperty(HEADERS).getValue();
                if (StringUtils.isNotEmpty(headersValue)) {
                    String[] headerPairs = headersValue.split(",");
                    for (String headerPair : headerPairs) {
                        if (headerPair.contains(":")) {
                            String[] parts = headerPair.split(":", 2);
                            httpGet.setHeader(parts[0].trim(), parts[1].trim());
                        }
                    }
                }
                
                // Add authentication headers
                String authType = context.getProperty(AUTH_TYPE).getValue();
                if (!"None".equalsIgnoreCase(authType)) {
                    switch (authType) {
                        case "Basic":
                            String username = context.getProperty(AUTH_USERNAME).getValue();
                            String password = context.getProperty(AUTH_PASSWORD).getValue();
                            if (StringUtils.isNotEmpty(username) && StringUtils.isNotEmpty(password)) {
                                String auth = username + ":" + password;
                                String encodedAuth = Base64.getEncoder().encodeToString(auth.getBytes(StandardCharsets.UTF_8));
                                httpGet.setHeader("Authorization", "Basic " + encodedAuth);
                            }
                            break;
                        case "Bearer":
                            String token = context.getProperty(AUTH_TOKEN).getValue();
                            if (StringUtils.isNotEmpty(token)) {
                                httpGet.setHeader("Authorization", "Bearer " + token);
                            }
                            break;
                        case "API Key":
                            String apiKeyHeaderName = context.getProperty(API_KEY_HEADER).getValue();
                            String apiKey = context.getProperty(AUTH_TOKEN).getValue();
                            if (StringUtils.isNotEmpty(apiKeyHeaderName) && StringUtils.isNotEmpty(apiKey)) {
                                httpGet.setHeader(apiKeyHeaderName, apiKey);
                            }
                            break;
                    }
                }
                
                // Execute request
                response = httpClient.execute(httpGet);
                int statusCode = response.getCode();
                
                // Get the HTTP entity
                HttpEntity entity = response.getEntity();
                
                if (entity != null) {
                    // Read the entity content
                    byte[] content = EntityUtils.toByteArray(entity);
                    
                    // Get the content type from the header
                    String contentType = response.getFirstHeader("Content-Type") != null
                            ? response.getFirstHeader("Content-Type").getValue()
                            : null;
                    
                    // If content type header is missing, try to detect it from the content
                    if (contentType == null || contentType.isEmpty()) {
                        contentType = detectContentType(content, currentItem.getUrl());
                    } else {
                        // Extract the base content type (without charset)
                        if (contentType.contains(";")) {
                            contentType = contentType.substring(0, contentType.indexOf(";")).trim();
                        }
                    }
                    
                    // Skip processing if specific MIME types are configured and this one isn't in the list
                    if (StringUtils.isNotEmpty(supportedMimeTypes)) {
                        String[] mimeTypes = supportedMimeTypes.split(",");
                        boolean isSupportedType = false;
                        
                        for (String mimeType : mimeTypes) {
                            if (contentType != null && contentType.trim().equalsIgnoreCase(mimeType.trim())) {
                                isSupportedType = true;
                                break;
                            }
                        }
                        
                        if (!isSupportedType) {
                            // Skip processing this content type
                            getLogger().debug("Skipping unsupported content type: " + contentType);
                            continue;
                        }
                    }
                    
                    // Create a FlowFile for the response
                    FlowFile flowFile = session.create();
                    flowFile = session.write(flowFile, out -> {
                        out.write(content);
                    });
                    
                    // Add attributes to the FlowFile
                    Map<String, String> attributes = new HashMap<>();
                    attributes.put("webcrawler.url", currentItem.getUrl());
                    attributes.put("webcrawler.depth", String.valueOf(currentItem.getDepth()));
                    attributes.put("webcrawler.parent.url", currentItem.getParentUrl());
                    attributes.put("webcrawler.status.code", String.valueOf(statusCode));
                    attributes.put("webcrawler.content.type", contentType != null ? contentType : "application/octet-stream");
                    attributes.put("webcrawler.timestamp", String.valueOf(System.currentTimeMillis()));
                    attributes.put("webcrawler.content.length", String.valueOf(content.length));
                    
                    flowFile = session.putAllAttributes(flowFile, attributes);
                    
                    // Transfer the FlowFile to success
                    session.transfer(flowFile, REL_SUCCESS);
                    session.getProvenanceReporter().receive(flowFile, currentItem.getUrl());
                    
                    // Extract links from the response based on content type
                    Set<String> extractedLinks = new HashSet<>();
                    
                    if (contentType != null) {
                        if (contentType.equalsIgnoreCase("application/json")) {
                            // Extract links from JSON
                            extractedLinks.addAll(extractJsonLinks(new String(content, StandardCharsets.UTF_8), 
                                                                  paginationLinkJsonPath, 
                                                                  resourceLinksJsonPath));
                        } else if (contentType.equalsIgnoreCase("text/html")) {
                            // Extract links from HTML
                            extractedLinks.addAll(extractHtmlLinks(new String(content, StandardCharsets.UTF_8), 
                                                                 htmlLinkSelector,
                                                                 currentItem.getUrl()));
                        } else if (contentType.equalsIgnoreCase("application/pdf")) {
                            // Extract links from PDF
                            extractedLinks.addAll(extractPdfLinks(content));
                        } else if (contentType.equalsIgnoreCase("text/plain")) {
                            // Extract links from plain text (using simple regex)
                            extractedLinks.addAll(extractLinksFromText(new String(content, StandardCharsets.UTF_8)));
                        } else if (contentType.equalsIgnoreCase("application/xml") || 
                                   contentType.equalsIgnoreCase("text/xml")) {
                            // Extract links from XML
                            extractedLinks.addAll(extractXmlLinks(new String(content, StandardCharsets.UTF_8)));
                        }
                    }
                    
                    // Add extracted links to the queue if they match the URL pattern
                    for (String link : extractedLinks) {
                        if (urlRegexPattern == null || urlRegexPattern.matcher(link).matches()) {
                            queue.add(new CrawlItem(link, currentItem.getDepth() + 1, currentItem.getUrl()));
                        }
                    }
                }
            } catch (Exception e) {
                getLogger().error("Failed to process URL: " + currentItem.getUrl(), e);
                
                // Create a FlowFile for the error
                FlowFile flowFile = session.create();
                
                // Add error details to the FlowFile
                Map<String, String> attributes = new HashMap<>();
                attributes.put("webcrawler.url", currentItem.getUrl());
                attributes.put("webcrawler.depth", String.valueOf(currentItem.getDepth()));
                attributes.put("webcrawler.parent.url", currentItem.getParentUrl());
                attributes.put("webcrawler.error", e.getMessage());
                attributes.put("webcrawler.timestamp", String.valueOf(System.currentTimeMillis()));
                
                flowFile = session.putAllAttributes(flowFile, attributes);
                
                // Transfer the FlowFile to failure
                session.transfer(flowFile, REL_FAILURE);
            } finally {
                try {
                    if (response != null) {
                        response.close();
                    }
                    if (httpClient != null) {
                        httpClient.close();
                    }
                } catch (IOException e) {
                    getLogger().error("Error closing HTTP client or response", e);
                }
            }
        }
    }
    
    /**
     * Detect the content type based on the content bytes and URL
     */
    private String detectContentType(byte[] content, String url) {
        // Check for common file signatures (magic numbers)
        if (content.length > 4) {
            // PDF signature
            if (content[0] == '%' && content[1] == 'P' && content[2] == 'D' && content[3] == 'F') {
                return "application/pdf";
            }
            
            // HTML signature
            if (content.length > 10) {
                String start = new String(content, 0, Math.min(content.length, 10), StandardCharsets.UTF_8).toLowerCase();
                if (start.contains("<!doctype") || start.contains("<html")) {
                    return "text/html";
                }
            }
            
            // XML signature
            if (content[0] == '<' && content[1] == '?' && content[2] == 'x' && content[3] == 'm' && content[4] == 'l') {
                return "application/xml";
            }
            
            // JSON signature
            if ((content[0] == '{' || content[0] == '[') && isJsonContent(content)) {
                return "application/json";
            }
        }
        
        // If we can't detect from signature, try using the URL extension
        if (url != null) {
            String lowercaseUrl = url.toLowerCase();
            if (lowercaseUrl.endsWith(".html") || lowercaseUrl.endsWith(".htm")) {
                return "text/html";
            } else if (lowercaseUrl.endsWith(".json")) {
                return "application/json";
            } else if (lowercaseUrl.endsWith(".pdf")) {
                return "application/pdf";
            } else if (lowercaseUrl.endsWith(".xml")) {
                return "application/xml";
            } else if (lowercaseUrl.endsWith(".txt")) {
                return "text/plain";
            }
        }
        
        // Default to octet-stream if we couldn't detect
        return "application/octet-stream";
    }
    
    /**
     * Check if the content is likely to be JSON
     */
    private boolean isJsonContent(byte[] content) {
        try {
            objectMapperRef.get().readTree(content);
            return true;
        } catch (Exception e) {
            return false;
        }
    }
    
    /**
     * Extract links from JSON content
     */
    private Set<String> extractJsonLinks(String jsonContent, String paginationLinkJsonPath, String resourceLinksJsonPath) {
        Set<String> links = new HashSet<>();
        
        if (StringUtils.isEmpty(jsonContent)) {
            return links;
        }
        
        try {
            JsonNode rootNode = objectMapperRef.get().readTree(jsonContent);
            
            // Extract the pagination link if the JSONPath is specified
            if (StringUtils.isNotEmpty(paginationLinkJsonPath)) {
                // Simple JSONPath implementation (this is a basic version)
                String[] pathParts = paginationLinkJsonPath.split("\\.");
                JsonNode currentNode = rootNode;
                
                for (String part : pathParts) {
                    if (part.startsWith("$")) {
                        // Root indicator, skip
                        continue;
                    } else if (part.endsWith("[*]")) {
                        // Array notation, not supported in pagination link
                        break;
                    } else {
                        currentNode = currentNode.get(part);
                        if (currentNode == null) {
                            break;
                        }
                    }
                }
                
                if (currentNode != null && currentNode.isTextual()) {
                    links.add(currentNode.asText());
                }
            }
            
            // Extract resource links if the JSONPath is specified
            if (StringUtils.isNotEmpty(resourceLinksJsonPath)) {
                // Simple JSONPath implementation with array support (this is a basic version)
                String[] pathParts = resourceLinksJsonPath.split("\\.");
                List<JsonNode> currentNodes = new ArrayList<>();
                currentNodes.add(rootNode);
                
                for (String part : pathParts) {
                    if (part.startsWith("$")) {
                        // Root indicator, skip
                        continue;
                    } else {
                        List<JsonNode> nextNodes = new ArrayList<>();
                        
                        for (JsonNode node : currentNodes) {
                            if (part.endsWith("[*]")) {
                                // Array notation
                                String fieldName = part.substring(0, part.length() - 3);
                                JsonNode arrayNode = node.get(fieldName);
                                
                                if (arrayNode != null && arrayNode.isArray()) {
                                    for (JsonNode item : arrayNode) {
                                        nextNodes.add(item);
                                    }
                                }
                            } else {
                                JsonNode nextNode = node.get(part);
                                if (nextNode != null) {
                                    nextNodes.add(nextNode);
                                }
                            }
                        }
                        
                        currentNodes = nextNodes;
                    }
                }
                
                for (JsonNode node : currentNodes) {
                    if (node.isTextual()) {
                        links.add(node.asText());
                    }
                }
            }
        } catch (Exception e) {
            // Log error but continue processing
            getLogger().error("Failed to extract links from JSON", e);
        }
        
        return links;
    }
    
    /**
     * Extract links from HTML content using JSoup
     */
    private Set<String> extractHtmlLinks(String htmlContent, String cssSelector, String baseUrl) {
        Set<String> links = new HashSet<>();
        
        if (StringUtils.isEmpty(htmlContent)) {
            return links;
        }
        
        try {
            Document doc = Jsoup.parse(htmlContent, baseUrl);
            
            // Extract links based on CSS selector
            Elements elements = doc.select(cssSelector);
            for (Element element : elements) {
                String href = element.attr("abs:href");
                if (StringUtils.isNotEmpty(href)) {
                    links.add(href);
                }
            }
            
            // Additional extraction for pagination links (common patterns)
            Elements paginationLinks = doc.select("a[rel=next], .pagination a, .pager a, nav.pagination a");
            for (Element element : paginationLinks) {
                String href = element.attr("abs:href");
                if (StringUtils.isNotEmpty(href)) {
                    links.add(href);
                }
            }
            
        } catch (Exception e) {
            // Log error but continue processing
            getLogger().error("Failed to extract links from HTML", e);
        }
        
        return links;
    }
    
    /**
     * Extract links from PDF content using Apache PDFBox
     */
    private Set<String> extractPdfLinks(byte[] pdfContent) {
        Set<String> links = new HashSet<>();
        
        if (pdfContent == null || pdfContent.length == 0) {
            return links;
        }
        
        try (PDDocument document = PDDocument.load(pdfContent)) {
            PDFTextStripper stripper = new PDFTextStripper();
            String text = stripper.getText(document);
            
            // Extract links using regex
            links.addAll(extractLinksFromText(text));
            
        } catch (Exception e) {
            // Log error but continue processing
            getLogger().error("Failed to extract links from PDF", e);
        }
        
        return links;
    }
    
    /**
     * Extract links from plain text using regex
     */
    private Set<String> extractLinksFromText(String text) {
        Set<String> links = new HashSet<>();
        
        if (StringUtils.isEmpty(text)) {
            return links;
        }
        
        // URL regex pattern
        Pattern urlPattern = Pattern.compile(
            "\\b(https?|ftp|file)://[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-a-zA-Z0-9+&@#/%=~_|]");
        
        Matcher matcher = urlPattern.matcher(text);
        while (matcher.find()) {
            links.add(matcher.group());
        }
        
        return links;
    }
    
    /**
     * Extract links from XML content using regex
     */
    private Set<String> extractXmlLinks(String xmlContent) {
        Set<String> links = new HashSet<>();
        
        if (StringUtils.isEmpty(xmlContent)) {
            return links;
        }
        
        try {
            // Look for href, url, link and src attributes in XML
            Pattern pattern = Pattern.compile("(href|url|link|src)=[\"']([^\"']+)[\"']");
            Matcher matcher = pattern.matcher(xmlContent);
            
            while (matcher.find()) {
                String url = matcher.group(2);
                // Only add if it looks like a URL
                if (url.startsWith("http://") || url.startsWith("https://") || url.startsWith("ftp://")) {
                    links.add(url);
                }
            }
        } catch (Exception e) {
            // Log error but continue processing
            getLogger().error("Failed to extract links from XML", e);
        }
        
        return links;
    }
    
    /**
     * Extract text from PDF content
     */
    public String extractTextFromPdf(byte[] pdfContent) {
        if (pdfContent == null || pdfContent.length == 0) {
            return "";
        }
        
        try (PDDocument document = PDDocument.load(pdfContent)) {
            PDFTextStripper stripper = new PDFTextStripper();
            return stripper.getText(document);
        } catch (Exception e) {
            getLogger().error("Failed to extract text from PDF", e);
            return "";
        }
    }
    
    /**
     * Inner class to represent a crawl item in the queue
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