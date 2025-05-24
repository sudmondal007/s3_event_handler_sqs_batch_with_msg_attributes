package com.pupu.home.s3.event.handler;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.lambda.runtime.logging.LogLevel;
import com.pupu.home.s3.event.processor.DataloadS3EventProcessor;

public class DataloadS3EventHandler implements RequestHandler<S3Event, Boolean> {

	@Override
	public Boolean handleRequest(S3Event s3Event, Context context) {
		LambdaLogger logger = context.getLogger();
		logger.log("DataloadS3EventHandler.handleRequest:: invoked", LogLevel.INFO);
		
		DataloadS3EventProcessor processor = new DataloadS3EventProcessor(s3Event, logger);
		processor.processDataloadS3Event();
		
		logger.log("DataloadS3EventHandler.handleRequest:: completed", LogLevel.INFO);
		return true;
	}
}
