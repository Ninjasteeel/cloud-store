package com.emse.TP;



import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SNSEvent;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

public class worker implements RequestHandler<SNSEvent, Void> {

    private static final Region region = Region.US_EAST_1;
    private static final String bucketName = "mybucket1pp1";
    private static final String outputFolder = "summaries/";

    @Override
    public Void handleRequest(SNSEvent event, Context context) {
        // Record the start time
        long startTime = System.currentTimeMillis();

        try (S3Client s3 = S3Client.builder().region(region).build()) {
            for (SNSEvent.SNSRecord record : event.getRecords()) {
                String message = record.getSNS().getMessage();
                String[] messageParts = message.split(";");
                String sourceBucket = messageParts[0];
                System.out.println(message);
                System.out.println(messageParts[0]);
                System.out.println(messageParts[1]);

                String sourceKey = messageParts[1];
                AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();

                // Download CSV file from S3
                final InputStream inputStream = s3.getObject(GetObjectRequest.builder()
                        .bucket(bucketName)
                        .key(sourceKey)
                        .build());

                // Process CSV file and generate summaries
                Map<String, StoreSummary> storeSummaries = processCsv(inputStream);

                // Store the summarized result in two new CSV files
                String storeSummaryKey = outputFolder + extractDate(sourceKey) + "store_summary_" + sourceKey;
                String productSummaryKey = outputFolder + extractDate(sourceKey) + "product_summary_" + sourceKey;

                PutObjectRequest storeSummaryRequest = PutObjectRequest.builder()
                        .bucket(bucketName)
                        .key(storeSummaryKey)
                        .build();
                PutObjectResponse storeSummaryResponse = s3.putObject(storeSummaryRequest, RequestBody.fromBytes(generateStoreSummaryCsv(storeSummaries).getBytes(StandardCharsets.UTF_8)));

                System.out.println("Store summary file stored at: s3://" + bucketName + "/" + storeSummaryKey);

                // Process the storeSummaries map to get product summaries
                Map<String, ProductSummary> productSummaries = generateProductSummaries(storeSummaries);

                PutObjectRequest productSummaryRequest = PutObjectRequest.builder()
                        .bucket(bucketName)
                        .key(productSummaryKey)
                        .build();
                PutObjectResponse productSummaryResponse = s3.putObject(productSummaryRequest, RequestBody.fromBytes(generateProductSummaryCsv(productSummaries).getBytes(StandardCharsets.UTF_8)));

                System.out.println("Product summary file stored at: s3://" + bucketName + "/" + productSummaryKey);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        // Record the end time
        long endTime = System.currentTimeMillis();

        // Calculate and print the execution time
        long executionTime = endTime - startTime;
        System.out.println("Execution Time: " + executionTime + " milliseconds");

        return null;
    }


    private Map<String, StoreSummary> processCsv(InputStream inputStream) throws IOException {
        Map<String, StoreSummary> storeSummaries = new HashMap<>();

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
            String line;
            // Skip header
            reader.readLine();

            while ((line = reader.readLine()) != null) {
                String[] columns = line.split(";");
                String store = columns[1];
                String product = columns[2];
                double quantity = Double.parseDouble(columns[3]);
                double unitProfit = Double.parseDouble(columns[6]);

                storeSummaries.computeIfAbsent(store, k -> new StoreSummary());
                storeSummaries.get(store).addProduct(product, quantity, unitProfit);
            }
        }

        return storeSummaries;
    }

    private String generateStoreSummaryCsv(Map<String, StoreSummary> storeSummaries) {
        StringBuilder result = new StringBuilder("Store,TotalProfitByStore\n");

        for (Map.Entry<String, StoreSummary> entry : storeSummaries.entrySet()) {
            result.append(entry.getKey()).append(",")
                    .append(entry.getValue().getTotalProfit()).append("\n");
        }

        return result.toString();
    }

    private Map<String, ProductSummary> generateProductSummaries(Map<String, StoreSummary> storeSummaries) {
        Map<String, ProductSummary> productSummaries = new HashMap<>();

        for (Map.Entry<String, StoreSummary> entry : storeSummaries.entrySet()) {
            StoreSummary storeSummary = entry.getValue();
            for (Map.Entry<String, ProductSummary> productEntry : storeSummary.getProductSummaries().entrySet()) {
                String productName = productEntry.getKey();
                ProductSummary existingProductSummary = productSummaries.getOrDefault(productName, new ProductSummary());
                ProductSummary productSummary = productEntry.getValue();

                existingProductSummary.setTotalQuantity(existingProductSummary.getTotalQuantity() + productSummary.getTotalQuantity());
                existingProductSummary.setTotalSold(existingProductSummary.getTotalSold() + productSummary.getTotalSold());
                existingProductSummary.setTotalProfitByProduct(existingProductSummary.getTotalProfitByProduct() + productSummary.getTotalProfitByProduct());

                productSummaries.put(productName, existingProductSummary);
            }
        }

        return productSummaries;
    }

    private String generateProductSummaryCsv(Map<String, ProductSummary> productSummaries) {
        StringBuilder result = new StringBuilder("Product,TotalQuantity,TotalSold,TotalProfitByProduct\n");

        for (Map.Entry<String, ProductSummary> entry : productSummaries.entrySet()) {
            ProductSummary productSummary = entry.getValue();
            result.append(entry.getKey()).append(",")
                    .append(productSummary.getTotalQuantity()).append(",")
                    .append(productSummary.getTotalSold()).append(",")
                    .append(productSummary.getTotalProfitByProduct()).append("\n");
        }

        return result.toString();
    }
    private String extractDate(String sourceKey) {
        // Assuming the format is "DD-MM-YYYY-store1"
        String[] parts = sourceKey.split("-");
        
        // Extract the date portion (DD-MM-YYYY)
        if (parts.length >= 2) {
            return parts[0] + "-" + parts[1] + "-" + parts[2];
        }
    
        // Return a default date or handle the case where the format is unexpected
        return "default-date";
    }
}


