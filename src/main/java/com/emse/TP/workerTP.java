package com.emse.TP ;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;

public class workerTP {
          
    
    private static final String queueUrl = "https://sqs.us-east-1.amazonaws.com/890272390288/tp-queue";
    private static final Region region = Region.US_EAST_1;
    private static final String bucketName = "mybucket1pp1";
    private static final String outputFolder = "summaries/";
    public static void main(String[] args) {
        SqsClient sqs = SqsClient.builder().region(region).build();
    
        String queueURL = "https://sqs.us-east-1.amazonaws.com/890272390288/tp-queue";
    
        // Record the start time
        long startTime = System.currentTimeMillis();
    
        ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder().queueUrl(queueURL).maxNumberOfMessages(1).build();
    
        // Receive messages from the SQS queue
        ReceiveMessageResponse receiveMessageResponse = sqs.receiveMessage(
                ReceiveMessageRequest.builder()
                        .queueUrl(queueURL)
                        .maxNumberOfMessages(1)
                        .waitTimeSeconds(20) // Long polling for better efficiency
                        .build()
        );
    
        for (Message message : receiveMessageResponse.messages()) {
            // Process the SQS message
            processSqsMessage(message.body());
    
            // Delete the message from the queue after processing
            sqs.deleteMessage(
                    DeleteMessageRequest.builder()
                            .queueUrl(queueURL)
                            .receiptHandle(message.receiptHandle())
                            .build()
            );
    
            // Break out of the loop after processing the first message
            break;
        }
    
        // Record the end time
        long endTime = System.currentTimeMillis();
    
        // Calculate and print the execution time
        long executionTime = endTime - startTime;
        System.out.println("Execution Time: " + executionTime + " milliseconds");
    }
     public static Void processSqsMessage(String message)  {
        try (S3Client s3 = S3Client.builder().region(region).build()) {
           
                String[] messageParts = message.split(";");
                String sourceKey = messageParts[1].split(",")[0].replace("\"", "");
                System.out.println(sourceKey);
                AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
               // Download CSV file from S3
               final InputStream inputStream = s3.getObject(GetObjectRequest.builder()
               .bucket(bucketName)
               .key(sourceKey)
               .build());

                // Process CSV file and generate summaries
                Map<String, StoreSummary> storeSummaries = processCsv(inputStream);

                // Store the summarized result in two new CSV files
                String storeSummaryKey = outputFolder +extractDate(sourceKey) +"store_summary_" + sourceKey;
                String productSummaryKey = outputFolder +extractDate(sourceKey)+"product_summary_" + sourceKey;

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
            
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    private static Map<String, StoreSummary> processCsv(InputStream inputStream) throws IOException {
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

    private static String generateStoreSummaryCsv(Map<String, StoreSummary> storeSummaries) {
        StringBuilder result = new StringBuilder("Store,TotalProfitByStore\n");

        for (Map.Entry<String, StoreSummary> entry : storeSummaries.entrySet()) {
            result.append(entry.getKey()).append(",")
                    .append(entry.getValue().getTotalProfit()).append("\n");
        }

        return result.toString();
    }

    private static Map<String, ProductSummary> generateProductSummaries(Map<String, StoreSummary> storeSummaries) {
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

    private static String generateProductSummaryCsv(Map<String, ProductSummary> productSummaries) {
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
    private static String extractDate(String sourceKey) {
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