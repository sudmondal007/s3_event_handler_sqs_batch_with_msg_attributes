package com.pupu.home.processor.sqs.queue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.logging.LogLevel;
import com.pupu.home.aws.client.factory.AWSClientFactory;
import com.pupu.home.dto.Member;
import com.pupu.home.utils.DataloadConstants;

import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequestEntry;

public class DataloadSQSQueueBatchProcessor {
	
	private static DataloadSQSQueueBatchProcessor instance;
	private DataloadSQSQueueBatchProcessor() {}
	public static DataloadSQSQueueBatchProcessor getInstance() {
		if (instance == null) {
			instance = new DataloadSQSQueueBatchProcessor();
		}
		return instance;
	}
	
	public void processMemberRecordsInChunk(List<Member> memberList, LambdaLogger logger) {
		logger.log("DataloadSQSQueueBatchSubmitter.processMemberRecordsInChunk:: START", LogLevel.INFO);
		
		if(memberList != null && memberList.size() > 0) {
			
			int chuckSize = 10;
			int dataSize = memberList.size();
			
			int chuckCounter = 1;
			
			for(int i=0; i<dataSize; i+= chuckSize) {
				int end = Math.min(i + chuckSize, dataSize);
				
				List<Member> chunk = memberList.subList(i, end);
				
				//submitSQSEventInBatch(chunk, chuckCounter, logger);
				
				chuckCounter ++;
			}
		}
		
	}
	
	private void submitSQSEventInBatch(List<Member> chunk, int chuckCounter, LambdaLogger logger) {
		logger.log("DataloadSQSQueueBatchSubmitter.submitSQSEventInBatch:: START", LogLevel.INFO);
		
		if(chunk != null && chunk.size() > 0) {
			List<SendMessageBatchRequestEntry> sqsBatchEntries = new ArrayList<>();
			
			for(Member member : chunk) {
				Map<String, MessageAttributeValue> sqsMessageAttributeEntry = buildMessageAttribute(member);
				
				SendMessageBatchRequestEntry batchRequestEntry = SendMessageBatchRequestEntry.builder()
						.id(UUID.randomUUID().toString())
						.messageBody(DataloadConstants.SQS_MSG_BODY)
						.messageGroupId(System.getenv(DataloadConstants.SQS_MSG_GRP_ID))
						.messageDeduplicationId(UUID.randomUUID().toString())
						.messageAttributes(sqsMessageAttributeEntry)
						.build();
				
				sqsBatchEntries.add(batchRequestEntry);
			}
			
			sendMessageToSQSBatch(sqsBatchEntries, chuckCounter, logger);
		}
	}
	
	private void sendMessageToSQSBatch(List<SendMessageBatchRequestEntry> sqsBatchEntries, int chuckCounter, LambdaLogger logger) {
		if(sqsBatchEntries != null && sqsBatchEntries.size() > 0) {
			SendMessageBatchRequest batchRequest = SendMessageBatchRequest.builder()
					.queueUrl(System.getenv(DataloadConstants.SQS_QUEUE_URL))
					.entries(sqsBatchEntries)
					.build();
			
			//AWSClientFactory.getInstance().getSqsClient().sendMessageBatch(batchRequest);
			logger.log("DataloadSQSQueueBatchSubmitter.sendMessageToSQSBatch:: submitted SQS BATCH for chuck=" + chuckCounter, LogLevel.INFO);
		}
	}
	
	private Map<String, MessageAttributeValue> buildMessageAttribute(Member member) {
		Map<String, MessageAttributeValue> temp = new HashMap<>();
		
		temp.put(DataloadConstants.DATAFIELD_FIRSTNAME, buildAndGetMessageAttributeValue(member.getFirstName()));
		temp.put(DataloadConstants.DATAFIELD_LASTNAME, buildAndGetMessageAttributeValue(member.getLastName()));
		
		return temp;
	}
	
	private MessageAttributeValue buildAndGetMessageAttributeValue(String value) {
		MessageAttributeValue messageAttributeValue = MessageAttributeValue.builder()
				.dataType(DataloadConstants.DATATYPE_STRING)
				.stringValue(value)
				.build();
		
		return messageAttributeValue;
	}
	
}
