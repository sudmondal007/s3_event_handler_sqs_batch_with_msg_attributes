package com.pupu.home.handler.s3.event;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.lambda.runtime.logging.LogLevel;
import com.pupu.home.processor.factory.LambdaProcessorFactory;
import com.pupu.home.processor.s3.event.DataloadS3EventProcessor;
import com.pupu.home.utils.LambdaProcssorType;

public class DataloadS3EventHandler implements RequestHandler<S3Event, Boolean> {
	
	@Override
	public Boolean handleRequest(S3Event s3Event, Context context) {
		LambdaLogger logger = context.getLogger();
		logger.log("DataloadS3EventHandler.handleRequest:: invoked", LogLevel.INFO);
		
		DataloadS3EventProcessor processor = (DataloadS3EventProcessor)LambdaProcessorFactory.getInstance().getProcessor(LambdaProcssorType.S3EVENT.name());
		processor.processDataloadS3Event(s3Event, logger);
		
		logger.log("DataloadS3EventHandler.handleRequest:: completed", LogLevel.INFO);
		return true;
	}
}
