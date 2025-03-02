package com.example.nifi.processors.webcrawler;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ArrayNode;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class RESTAPIWebCrawlerIT {

    private static HttpServer server;
    private static final Map<String, String> API_RESPONSES = new HashMap<>();
    private static final int PORT = 18080;
    private static ObjectMapper objectMapper = new ObjectMapper();
    
    @BeforeAll
    public static void setupServer() throws Exception {
        // Create and start HTTP server
        server = HttpServer.create(new InetSocketAddress(PORT), 0);
        
        // Create API responses
        setupApiResponses();
        
        // API context handler
        server.createContext("/api", new HttpHandler() {
            @Override
            public void handle(HttpExchange exchange) throws IOException {
                String path = exchange.getRequestURI().getPath();
                String query = exchange.getRequestURI().getQuery();
                String fullPath = path + (query != null ? "?" + query : "");
                
                String response = API_RESPONSES.getOrDefault(fullPath, "{\"error\": \"Not found\"}");
                int responseCode = API_RESPONSES.containsKey(fullPath) ? 200 : 404;
                
                exchange.getResponseHeaders().add("Content-Type", "application/json");
                exchange.sendResponseHeaders(responseCode, response.getBytes().length);
                
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(response.getBytes());
                }
            }
        });
        
        server.setExecutor(null);
        server.start();
    }
    
    private static void setupApiResponses() throws Exception {
        // Base API response with pagination and item links
        ObjectNode baseResponse = objectMapper.createObjectNode();
        ArrayNode items = baseResponse.putArray("items");
        
        for (int i = 1; i <= 3; i++) {
            ObjectNode item = items.addObject();
            item.put("id", i);
            item.put("name", "Item " + i);
            item.put("detail", "/api/items/" + i);
        }
        
        ObjectNode links = baseResponse.putObject("links");
        links.put("next", "/api/items?page=2");
        
        API_RESPONSES.put("/api/items", baseResponse.toString());
        
        // Page 2 response
        ObjectNode page2Response = objectMapper.createObjectNode();
        ArrayNode page2Items = page2Response.putArray("items");
        
        for (int i = 4; i <= 6; i++) {
            ObjectNode item = page2Items.addObject();
            item.put("id", i);
            item.put("name", "Item " + i);
            item.put("detail", "/api/items/" + i);
        }
        
        API_RESPONSES.put("/api/items?page=2", page2Response.toString());
        
        // Item detail responses
        for (int i = 1; i <= 6; i++) {
            ObjectNode itemDetail = objectMapper.createObjectNode();
            itemDetail.put("id", i);
            itemDetail.put("name", "Item " + i);
            itemDetail.put("description", "Detailed description for item " + i);
            
            API_RESPONSES.put("/api/items/" + i, itemDetail.toString());
        }
    }
    
    @AfterAll
    public static void tearDownServer() {
        if (server != null) {
            server.stop(0);
        }
    }
    
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testRecursiveCrawling() {
        TestRunner runner = TestRunners.newTestRunner(RESTAPIWebCrawler.class);
        
        // Configure processor
        runner.setProperty(RESTAPIWebCrawler.BASE_URL, "http://localhost:" + PORT + "/api/items");
        runner.setProperty(RESTAPIWebCrawler.MAX_DEPTH, "1");
        runner.setProperty(RESTAPIWebCrawler.PAGINATION_LINK_JSONPATH, "$.links.next");
        runner.setProperty(RESTAPIWebCrawler.RESOURCE_LINKS_JSONPATH, "$.items[*].detail");
        
        // Run the processor
        runner.run();
        
        // Verify results - should have 10 successful flowfiles
        // 1 for base URL, 1 for page 2, and 6 for individual items (with page 2 items having depth 1)
        List<MockFlowFile> successFiles = runner.getFlowFilesForRelationship(RESTAPIWebCrawler.REL_SUCCESS);
        assertTrue(successFiles.size() >= 8, "Expected at least 8 success files but got " + successFiles.size());
        
        // Verify that we have the expected URLs and depths
        boolean foundBaseUrl = false;
        boolean foundPage2 = false;
        int item1Count = 0;
        int itemsWithDepth0 = 0;
        int itemsWithDepth1 = 0;
        
        for (MockFlowFile flowFile : successFiles) {
            String url = flowFile.getAttribute("webcrawler.url");
            String depth = flowFile.getAttribute("webcrawler.depth");
            
            if (url.equals("http://localhost:" + PORT + "/api/items")) {
                foundBaseUrl = true;
                assertEquals("0", depth);
            } else if (url.equals("http://localhost:" + PORT + "/api/items?page=2")) {
                foundPage2 = true;
                assertEquals("1", depth);
            } else if (url.equals("http://localhost:" + PORT + "/api/items/1")) {
                item1Count++;
            }
            
            if (depth.equals("0")) {
                itemsWithDepth0++;
            } else if (depth.equals("1")) {
                itemsWithDepth1++;
            }
        }
        
        assertTrue(foundBaseUrl, "Base URL should be crawled");
        assertTrue(foundPage2, "Pagination URL should be crawled");
        assertEquals(1, item1Count, "Item 1 should be crawled exactly once");
        assertTrue(itemsWithDepth0 >= 4, "Should have at least 4 items with depth 0");
        assertTrue(itemsWithDepth1 >= 4, "Should have at least 4 items with depth 1");
        
        // Verify the content of one of the detail responses
        boolean foundDetailContent = false;
        for (MockFlowFile flowFile : successFiles) {
            String url = flowFile.getAttribute("webcrawler.url");
            if (url.equals("http://localhost:" + PORT + "/api/items/1")) {
                String content = new String(flowFile.toByteArray());
                assertTrue(content.contains("Detailed description for item 1"));
                foundDetailContent = true;
                break;
            }
        }
        
        assertTrue(foundDetailContent, "Should find content for item 1 detail");
    }
    
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testUrlPatternFiltering() {
        TestRunner runner = TestRunners.newTestRunner(RESTAPIWebCrawler.class);
        
        // Configure processor with URL pattern to only crawl odd-numbered items
        runner.setProperty(RESTAPIWebCrawler.BASE_URL, "http://localhost:" + PORT + "/api/items");
        runner.setProperty(RESTAPIWebCrawler.MAX_DEPTH, "1");
        runner.setProperty(RESTAPIWebCrawler.PAGINATION_LINK_JSONPATH, "$.links.next");
        runner.setProperty(RESTAPIWebCrawler.RESOURCE_LINKS_JSONPATH, "$.items[*].detail");
        runner.setProperty(RESTAPIWebCrawler.URL_PATTERN, ".*/items/[135]$");
        
        // Run the processor
        runner.run();
        
        // Verify results - should only crawl items 1, 3, 5
        List<MockFlowFile> successFiles = runner.getFlowFilesForRelationship(RESTAPIWebCrawler.REL_SUCCESS);
        assertTrue(successFiles.size() >= 5, "Expected at least 5 success files but got " + successFiles.size());
        
        // Check that only odd-numbered items were crawled
        boolean foundItem1 = false;
        boolean foundItem2 = false;
        boolean foundItem3 = false;
        boolean foundItem4 = false;
        boolean foundItem5 = false;
        
        for (MockFlowFile flowFile : successFiles) {
            String url = flowFile.getAttribute("webcrawler.url");
            
            if (url.equals("http://localhost:" + PORT + "/api/items/1")) {
                foundItem1 = true;
            } else if (url.equals("http://localhost:" + PORT + "/api/items/2")) {
                foundItem2 = true;
            } else if (url.equals("http://localhost:" + PORT + "/api/items/3")) {
                foundItem3 = true;
            } else if (url.equals("http://localhost:" + PORT + "/api/items/4")) {
                foundItem4 = true;
            } else if (url.equals("http://localhost:" + PORT + "/api/items/5")) {
                foundItem5 = true;
            }
        }
        
        assertTrue(foundItem1, "Item 1 should be crawled");
        assertFalse(foundItem2, "Item 2 should not be crawled");
        assertTrue(foundItem3, "Item 3 should be crawled");
        assertFalse(foundItem4, "Item 4 should not be crawled");
        assertTrue(foundItem5, "Item 5 should be crawled");
    }
} 