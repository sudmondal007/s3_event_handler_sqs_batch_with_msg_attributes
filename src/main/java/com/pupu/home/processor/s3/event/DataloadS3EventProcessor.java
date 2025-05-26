package com.pupu.home.processor.s3.event;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.lambda.runtime.logging.LogLevel;
import com.pupu.home.aws.client.factory.AWSClientFactory;
import com.pupu.home.dto.Member;
import com.pupu.home.processor.LambdaProcessor;
import com.pupu.home.processor.factory.LambdaProcessorFactory;
import com.pupu.home.processor.sqs.queue.DataloadSQSQueueBatchProcessor;
import com.pupu.home.utils.AWSClientType;
import com.pupu.home.utils.DataloadConstants;
import com.pupu.home.utils.LambdaProcssorType;

import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.sqs.SqsClient;

public class DataloadS3EventProcessor implements LambdaProcessor {
	
	public void processDataloadS3Event(S3Event s3Event, LambdaLogger logger) {
		logger.log("DataloadS3EventHandler.processDataloadS3Event:: START", LogLevel.INFO);
		
		// get bucket and key from the S3 event
		String bucket = s3Event.getRecords().get(0).getS3().getBucket().getName();
		String key = s3Event.getRecords().get(0).getS3().getObject().getKey();
		
		// get S3 object from event based on the bucket and the key
		ResponseBytes<GetObjectResponse> objectBytes = processAndGetS3FileObject(bucket, key, logger);
		
		// process S3 file and return the list of object
		List<Member> memberList = processS3File(objectBytes, logger);
		
		// process member records and send SQS Message
		DataloadSQSQueueBatchProcessor batchSubmitter = (DataloadSQSQueueBatchProcessor)LambdaProcessorFactory.getInstance().getProcessor(LambdaProcssorType.SQSBATCH.name());
		batchSubmitter.processMemberRecordsInChunk(memberList, logger);
	}
	
	/**
	 * Retrieve S3 object and return
	 * @param bucket
	 * @param key
	 * @return
	 */
	private ResponseBytes<GetObjectResponse> processAndGetS3FileObject(String bucket, String key, LambdaLogger logger) {
		logger.log("DataloadS3EventHandler.processAndGetS3FileObject:: START", LogLevel.INFO);
		ResponseBytes<GetObjectResponse> objectBytes = null;
		
		try {
			GetObjectRequest objectRequest = GetObjectRequest.builder().key(key).bucket(bucket).build();
			objectBytes = getS3Client().getObjectAsBytes(objectRequest);
		} catch (Exception ex) {
			logger.log("DataloadS3EventHandler.processAndGetS3FileObject:: error" + ex.getMessage(), LogLevel.ERROR);
		}
		
		return objectBytes;
	}
	
	/**
	 * process S3 object and return member data list
	 * @param objectBytes
	 * @return
	 */
	private List<Member> processS3File(ResponseBytes<GetObjectResponse> objectBytes, LambdaLogger logger) {
		logger.log("DataloadS3EventHandler.processS3File:: START", LogLevel.INFO);
		List<Member> memberList = null;
		
		if(objectBytes != null) {
			try(BufferedReader reader = new BufferedReader(new InputStreamReader(objectBytes.asInputStream()))) {
				
				String line = null;
				memberList = new ArrayList<Member>();
				
				while((line = reader.readLine()) != null) {
					if(line.equalsIgnoreCase(DataloadConstants.FILE_HEADER)) {
						continue;
					}
					String[] temp = line.split(DataloadConstants.COMMA);
					memberList.add(new Member(temp[0], temp[1]));
				}
			} catch(Exception ex) {
				logger.log("DataloadS3EventHandler.processS3File:: error" + ex.getMessage(), LogLevel.ERROR);
			}
		}
		return memberList;
	}

	private S3Client getS3Client() {
		S3Client s3Client = (S3Client)AWSClientFactory.getInstance().getClient(AWSClientType.S3CLIENT.name());
		return s3Client;
	}

}
