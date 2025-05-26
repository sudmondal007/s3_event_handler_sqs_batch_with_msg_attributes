package com.pupu.home.processor.factory;

import com.pupu.home.processor.LambdaProcessor;
import com.pupu.home.processor.s3.event.DataloadS3EventProcessor;
import com.pupu.home.processor.sqs.queue.DataloadSQSQueueBatchProcessor;
import com.pupu.home.utils.LambdaProcssorType;

public class LambdaProcessorFactory {
	private static LambdaProcessorFactory instance;

	private LambdaProcessorFactory() {
	}

	public static LambdaProcessorFactory getInstance() {
		if (instance == null) {
			synchronized (LambdaProcessorFactory.class) {
				if (instance == null) {
					instance = new LambdaProcessorFactory();
				}
			}
		}
		return instance;
	}

	public LambdaProcessor getProcessor(String type) {
		if (LambdaProcssorType.S3EVENT.name().equals(type)) {
			return new DataloadS3EventProcessor();
		} else if (LambdaProcssorType.SQSBATCH.name().equals(type)) {
			return new DataloadSQSQueueBatchProcessor();
		}
		throw new IllegalArgumentException("Unknown product type: " + type);
	}
}
