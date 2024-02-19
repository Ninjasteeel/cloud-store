package com.emse.TP;

public class ProductSummary {
    private String name;
    private double totalQuantity;
    private double totalSold;
    private double totalProfitByProduct;

    public void addProduct(double quantity, double unitProfit) {
        totalQuantity += quantity;
        totalSold += 1; // Assuming each line in the CSV represents a sold item
        totalProfitByProduct += quantity * unitProfit;
    }

     public String getName() {
        return name;
    }
    public void setName(String name) {
        this.name = name;
    }
    public void setTotalQuantity(double totalQuantity) {
        this.totalQuantity = totalQuantity;
    }

    public void setTotalSold(double totalSold) {
        this.totalSold = totalSold;
    }

    public void setTotalProfitByProduct(double totalProfitByProduct) {
        this.totalProfitByProduct = totalProfitByProduct;
    }

    public double getTotalQuantity() {
        return totalQuantity;
    }

    public double getTotalSold() {
        return totalSold;
    }

    public double getTotalProfitByProduct() {
        return totalProfitByProduct;
    }

    @Override
    public String toString() {
        return 
                "totalQuantity=" + totalQuantity +
                "; totalSold=" + totalSold +
                "; totalProfitByProduct=" + totalProfitByProduct 
                ;
    }
}

