package com.pupu.home.sqs.queue.submitter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.logging.LogLevel;
import com.pupu.home.s3.event.dto.Member;
import com.pupu.home.s3.event.utils.DataloadConstants;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequestEntry;

public class DataloadSQSQueueBatchSubmitter {
	
	public DataloadSQSQueueBatchSubmitter() {}
	
	public DataloadSQSQueueBatchSubmitter(List<Member> memberList, LambdaLogger logger) {
		this.memberList = memberList;
		this.logger = logger;
	}
	
	public void processMemberRecordsInChunk() {
		getLogger().log("DataloadSQSQueueBatchSubmitter.processMemberRecordsInChunk:: START", LogLevel.INFO);
		
		if(getMemberList() != null && getMemberList().size() > 0) {
			
			int chuckSize = 10;
			int dataSize = getMemberList().size();
			
			int chuckCounter = 1;
			
			for(int i=0; i<dataSize; i+= chuckSize) {
				int end = Math.min(i + chuckSize, dataSize);
				
				List<Member> chunk = getMemberList().subList(i, end);
				
				submitSQSEventInBatch(chunk, chuckCounter);
				
				chuckCounter ++;
			}
		}
		
	}
	
	private void submitSQSEventInBatch(List<Member> chunk, int chuckCounter) {
		getLogger().log("DataloadSQSQueueBatchSubmitter.submitSQSEventInBatch:: START", LogLevel.INFO);
		
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
			
			sendMessageToSQSBatch(sqsBatchEntries, chuckCounter);
		}
	}
	
	private void sendMessageToSQSBatch(List<SendMessageBatchRequestEntry> sqsBatchEntries, int chuckCounter) {
		if(sqsBatchEntries != null && sqsBatchEntries.size() > 0) {
			SendMessageBatchRequest batchRequest = SendMessageBatchRequest.builder()
					.queueUrl(System.getenv(DataloadConstants.SQS_QUEUE_URL))
					.entries(sqsBatchEntries)
					.build();
			
			getSqsClient().sendMessageBatch(batchRequest);
			getLogger().log("DataloadSQSQueueBatchSubmitter.sendMessageToSQSBatch:: submitted SQS BATCH for chuck=" + chuckCounter, LogLevel.INFO);
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
	
	//SQS Client
	private SqsClient sqsClient = SqsClient.builder()
			.region(Region.of(System.getenv(DataloadConstants.DATALOAD_AWS_REGION)))
			.build();
	
	private List<Member> memberList;
	private LambdaLogger logger;
	
	public SqsClient getSqsClient() {
		return sqsClient;
	}

	public List<Member> getMemberList() {
		return memberList;
	}

	public LambdaLogger getLogger() {
		return logger;
	}

}
