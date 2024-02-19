# Project README

## Project Title

Cloud Application for Retailer Management

## Project Description

This project implements a Java EC2 application and an AWS Lambda function to process messages from an SQS queue and SNS, download CSV files from an S3 bucket, generate store and product summaries, and store the results back in the S3 bucket,and also give analytics.

## Project Structure

The project is organized as follows:

- `src`: Contains the Java source code
  - `com.emse.TP`: Package for the project classes
    - `workerTP.java`: Main Java class for processing by EC2 implementation daily store sales and upload summaries .
- `pom.xml`: Maven Project Object Model file
- `README.md`: Project documentation (you are here)
- `worker.java`: Main Java class for processing daily as lambda function store sales and upload summaries .
- `client.java`: Main Java class for uploading daily store sailes in the bucket.
- `consolidator.java`: Main Java class for processing summaries and give analytics .
## Prerequisites

Before running the application or Lambda function, ensure the following:

- AWS CLI is configured with the necessary credentials.
- The required AWS services (SQS, S3) are set up, and the appropriate permissions are granted.

## How to Run the Java Application

1. Open a terminal window and navigate to the project directory.
2. Run the following command to build and run the project:

   ```bash
   mvn package
  
