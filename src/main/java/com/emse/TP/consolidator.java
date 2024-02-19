package com.emse.TP;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Comparator;
import java.util.Map;



import java.util.HashMap;


import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;



public class consolidator {

    private static final String bucketName = "mybucket1pp1"; // Replace with your actual S3 bucket name
    private static final String summaryFolder = "summaries/";

    public static void main(String[] args) {

        String date = "01-10-2022";

        try (S3Client s3 = S3Client.builder().region(Region.US_EAST_1).build()) {

            Map<String, StoreSummary> storeSummaries = new HashMap<>();
            Map<String, ProductSummary> productSummaries = new HashMap<>();

            // List all objects in the summaries folder for the given date
            ListObjectsV2Request listObjectsRequest = ListObjectsV2Request.builder()
                    .bucket(bucketName)
                    .prefix(summaryFolder + date)
                    .build();

            ListObjectsV2Response listObjectsResponse = s3.listObjectsV2(listObjectsRequest);

            for (S3Object s3Object : listObjectsResponse.contents()) {
                // Download CSV file from S3
                final InputStream inputStream = s3.getObject(GetObjectRequest.builder()
                        .bucket(bucketName)
                        .key(s3Object.key())
                        .build());

                BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));

                // Identify the type of summary file and update summaries accordingly
                if (s3Object.key().contains("store_summary")) {
                    updateStoreSummaries(reader, storeSummaries);
                } else if (s3Object.key().contains("product_summary")) {
                    updateProductSummaries(reader, productSummaries);
                }
            }

            // Display consolidated information
            displayTotalRetailerProfit(storeSummaries);
            displayMostAndLeastProfitableStores(storeSummaries);
            displayTotalQuantitySoldProfitPerProduct(productSummaries);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void updateStoreSummaries(BufferedReader reader, Map<String, StoreSummary> storeSummaries) throws IOException {
        String line;

        // Skip the header line
        reader.readLine();

        while ((line = reader.readLine()) != null) {
            String[] columns = line.split(",");

            // Ensure the columns array has enough elements
            if (columns.length >= 2) {
                String store = columns[0];
                double totalProfitByStore = Double.parseDouble(columns[1]);

                // Create a new StoreSummary or update an existing one
                StoreSummary storeSummary = storeSummaries.computeIfAbsent(store, k -> new StoreSummary());
                storeSummary.setTotalProfit(storeSummary.getTotalProfit() + totalProfitByStore);
            }
        }
    }

    private static void updateProductSummaries(BufferedReader reader, Map<String, ProductSummary> productSummaries) throws IOException {
        String line;

        // Skip the header line
        reader.readLine();

        while ((line = reader.readLine()) != null) {
            String[] columns = line.split(",");

            // Ensure the columns array has enough elements
            if (columns.length >= 4) {
                String product = columns[0];
                double totalQuantity = Double.parseDouble(columns[1]);
                double totalSold = Double.parseDouble(columns[2]);
                double totalProfitByProduct = Double.parseDouble(columns[3]);

                // Create a new ProductSummary or update an existing one
                ProductSummary productSummary = productSummaries.computeIfAbsent(product, k -> new ProductSummary());
                productSummary.setTotalQuantity(productSummary.getTotalQuantity() + totalQuantity);
                productSummary.setTotalSold(productSummary.getTotalSold() + totalSold);
                productSummary.setTotalProfitByProduct(productSummary.getTotalProfitByProduct() + totalProfitByProduct);
            }
        }
    }

    private static void displayTotalRetailerProfit(Map<String, StoreSummary> storeSummaries) {
        double totalRetailerProfit = storeSummaries.values().stream()
                .mapToDouble(StoreSummary::getTotalProfit)
                .sum();
        System.out.println("Total Retailer's Profit: " + totalRetailerProfit);
    }

    private static void displayMostAndLeastProfitableStores(Map<String, StoreSummary> storeSummaries) {
        if (!storeSummaries.isEmpty()) {
            String mostProfitableStore = storeSummaries.entrySet().stream()
                    .max(Comparator.comparingDouble(entry -> entry.getValue().getTotalProfit()))
                    .map(Map.Entry::getKey)
                    .orElse("N/A");

            String leastProfitableStore = storeSummaries.entrySet().stream()
                    .min(Comparator.comparingDouble(entry -> entry.getValue().getTotalProfit()))
                    .map(Map.Entry::getKey)
                    .orElse("N/A");

            System.out.println("Most Profitable Store: " + mostProfitableStore);
            System.out.println("Least Profitable Store: " + leastProfitableStore);
        } else {
            System.out.println("No store summaries found.");
        }
    }

    private static void displayTotalQuantitySoldProfitPerProduct(Map<String, ProductSummary> productSummaries) {
        productSummaries.forEach((product, productSummary) -> {
            System.out.println("Product: " + product);
            System.out.println("Total Quantity: " + productSummary.getTotalQuantity());
            System.out.println("Total Sold: " + productSummary.getTotalSold());
            System.out.println("Total Profit By Product: " + productSummary.getTotalProfitByProduct());
            System.out.println("------------------------------");
        });
    }
}