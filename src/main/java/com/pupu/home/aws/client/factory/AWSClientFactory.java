package com.pupu.home.aws.client.factory;

import com.pupu.home.utils.AWSClientType;
import com.pupu.home.utils.DataloadConstants;

import software.amazon.awssdk.awscore.AwsClient;
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
			synchronized (AWSClientFactory.class) {
				if (instance == null) {
					instance = new AWSClientFactory();
				}
			}
		}
		return instance;
	}

	public AwsClient getClient(String type) {
		if (AWSClientType.S3CLIENT.name().equals(type)) {
			return s3Client;
		} else if (AWSClientType.SQSCLIENT.name().equals(type)) {
			return sqsClient;
		}
		throw new IllegalArgumentException("Unknown client type: " + type);
	}
}
