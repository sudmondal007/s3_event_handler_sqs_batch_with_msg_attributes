package com.pupu.home.s3.event.processor;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.lambda.runtime.logging.LogLevel;
import com.pupu.home.s3.event.dto.Member;
import com.pupu.home.s3.event.utils.DataloadConstants;
import com.pupu.home.sqs.queue.submitter.DataloadSQSQueueBatchSubmitter;

import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

public class DataloadS3EventProcessor {
	
	// S3 client
	private S3Client s3Client = S3Client.builder()
			.region(Region.of(System.getenv(DataloadConstants.DATALOAD_AWS_REGION)))
			.build();
	
	private S3Event s3Event;
	private LambdaLogger logger;
	
	public DataloadS3EventProcessor() {}
	
	public DataloadS3EventProcessor(S3Event s3Event, LambdaLogger logger) {
		this.s3Event = s3Event;
		this.logger = logger;
	}
	
	public void processDataloadS3Event() {
		getLogger().log("DataloadS3EventHandler.processDataloadS3Event:: START", LogLevel.INFO);
		
		// get bucket and key from the S3 event
		String bucket = getS3Event().getRecords().get(0).getS3().getBucket().getName();
		String key = getS3Event().getRecords().get(0).getS3().getObject().getKey();
		
		// get S3 object from event based on the bucket and the key
		ResponseBytes<GetObjectResponse> objectBytes = processAndGetS3FileObject(bucket, key);
		
		// process S3 file and return the list of object
		List<Member> memberList = processS3File(objectBytes);
		
		// process member records and send SQS Message
		DataloadSQSQueueBatchSubmitter batchSubmitter = new DataloadSQSQueueBatchSubmitter(memberList, getLogger());
		batchSubmitter.processMemberRecordsInChunk();
	}
	
	/**
	 * Retrieve S3 object and return
	 * @param bucket
	 * @param key
	 * @return
	 */
	private ResponseBytes<GetObjectResponse> processAndGetS3FileObject(String bucket, String key) {
		getLogger().log("DataloadS3EventHandler.processAndGetS3FileObject:: START", LogLevel.INFO);
		ResponseBytes<GetObjectResponse> objectBytes = null;
		
		try {
			GetObjectRequest objectRequest = GetObjectRequest.builder().key(key).bucket(bucket).build();
			objectBytes = getS3Client().getObjectAsBytes(objectRequest);
		} catch (Exception ex) {
			getLogger().log("DataloadS3EventHandler.processAndGetS3FileObject:: error" + ex.getMessage(), LogLevel.ERROR);
		}
		
		return objectBytes;
	}
	
	/**
	 * process S3 object and return member data list
	 * @param objectBytes
	 * @return
	 */
	private List<Member> processS3File(ResponseBytes<GetObjectResponse> objectBytes) {
		getLogger().log("DataloadS3EventHandler.processS3File:: START", LogLevel.INFO);
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
				getLogger().log("DataloadS3EventHandler.processS3File:: error" + ex.getMessage(), LogLevel.ERROR);
			}
		}
		return memberList;
	}

	public S3Client getS3Client() {
		return s3Client;
	}

	public S3Event getS3Event() {
		return s3Event;
	}

	public LambdaLogger getLogger() {
		return logger;
	}

}
