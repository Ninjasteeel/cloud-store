package com.emse.TP;


import java.nio.file.Paths;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.PublishRequest;
import software.amazon.awssdk.services.sns.model.PublishResponse;


public class client {
    public static void main(String[] args) {
        Region region = Region.US_EAST_1;
        String topicARN = "arn:aws:sns:us-east-1:890272390288:TP_Topic";
        String fileName = "01-10-2022-store9";
        String bucketName = "mybucket1pp1";
        String path = "C:\\Users\\lenovo\\aws-cloud1\\src\\main\\java\\com\\emse\\TP\\"+fileName+".csv";
        try (S3Client s3 = S3Client.builder().region(region).build();

        SnsClient snsClient = SnsClient.builder().region(region).build()
        ) {

            // CSV file name passed as argument
            String key =   fileName;

            // Upload file to S3 bucket
            PutObjectRequest putRequest = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(key)
                .build();
            s3.putObject(putRequest,  Paths.get(path));

            // Notify worker application via SNS
           PublishRequest request = PublishRequest.builder().message(bucketName + ";" + fileName).topicArn(topicARN)
					.build();

           PublishResponse snsResponse = snsClient.publish(request);
			System.out.println(
					snsResponse.messageId() + " Message sent. Status is " + snsResponse.sdkHttpResponse().statusCode());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}