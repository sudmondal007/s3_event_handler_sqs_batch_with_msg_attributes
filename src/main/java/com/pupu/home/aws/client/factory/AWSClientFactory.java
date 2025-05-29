package com.pupu.home.aws.client.factory;

import com.pupu.home.utils.DataloadConstants;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;

public class AWSClientFactory {
	
	// S3 client
	private S3Client s3Client = S3Client.builder()
			.region(Region.of(System.getenv(DataloadConstants.DATALOAD_AWS_REGION)))
			.build();
	
	//SQS Client
	private SqsClient sqsClient = SqsClient.builder()
			.region(Region.of(System.getenv(DataloadConstants.DATALOAD_AWS_REGION)))
			.build();
		
	private static AWSClientFactory instance;

	private AWSClientFactory() {
	}

	public static AWSClientFactory getInstance() {
		if (instance == null) {
			instance = new AWSClientFactory();
		}
		return instance;
	}
	
	public S3Client getS3Client() {
		return s3Client;
	}
	
	public SqsClient getSqsClient() {
		return sqsClient;
	}

}
