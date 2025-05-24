Process S3 Events based on a CSV file uploaded
process each record 
send queue message to SQS queue in batch of 5 queues with data fields as message attributes
the SQS queue should be FIFO for this example
