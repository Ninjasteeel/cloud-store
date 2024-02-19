package com.emse.TP;

import java.util.HashMap;
import java.util.Map;

public class StoreSummary {
    private double totalProfit;
    private Map<String, ProductSummary> productSummaries = new HashMap<>();

    public void addProduct(String product, double quantity, double unitProfit) {
        totalProfit += quantity * unitProfit;

        productSummaries.computeIfAbsent(product, k -> new ProductSummary()).addProduct(quantity, unitProfit);
    }

    public double getTotalProfit() {
        return totalProfit;
    }

    public void setTotalProfit(double totalProfit) {
        this.totalProfit = totalProfit;
    }

    public Map<String, ProductSummary> getProductSummaries() {
        return productSummaries;
    }

    // Getter method for totalSold
    public double getTotalSold() {
        return productSummaries.values().stream().mapToDouble(ProductSummary::getTotalSold).sum();
    }

    // Getter method for totalQuantity
    public double getTotalQuantity() {
        return productSummaries.values().stream().mapToDouble(ProductSummary::getTotalQuantity).sum();
    }

    @Override
    public String toString() {
        StringBuilder result = new StringBuilder();
        result.append("StoreSummary{totalProfit=").append(totalProfit).append(", productSummaries={");
        productSummaries.forEach((product, productSummary) ->
                result.append(product).append(";").append(productSummary).append("; "));
        result.delete(result.length() - 2, result.length()); // Remove the trailing comma and space
        result.append("}}");
        return result.toString();
    }
}
