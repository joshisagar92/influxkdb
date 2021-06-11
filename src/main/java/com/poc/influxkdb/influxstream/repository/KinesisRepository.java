package com.poc.influxkdb.influxstream.repository;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import com.amazonaws.services.kinesis.model.PutRecordsResultEntry;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.poc.influxkdb.influxstream.service.InfluxWriterService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Repository;

import javax.annotation.PostConstruct;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;

@Repository
public class KinesisRepository {

    public static final String AWS_ACCESS_KEY = "aws.accessKeyId";
    public static final String AWS_SECRET_KEY = "aws.secretKey";


    private static Logger logger = LoggerFactory.getLogger(InfluxWriterService.class);

    @Value("${application.aws.kinesistopic}")
    private String kinesis_topic;

    @Value("${application.aws.accesskey}")
    private  String ACCESS_KEY;


    @Value("${application.aws.secret}")
    private  String SECRET;

    @Value("${application.aws.region}")
    private String region;

    @Value("${record.batch.threshold}")
    private int record_batch_threshold;


    @PostConstruct
    public void setUp(){
        logger.debug("*********************************");
        logger.debug(ACCESS_KEY);
        logger.debug(SECRET);
        logger.debug("**********************************");

        System.setProperty(AWS_ACCESS_KEY, ACCESS_KEY);
        System.setProperty(AWS_SECRET_KEY, SECRET);
    }

    public void send(BlockingQueue<JsonObject> queue) {

        AmazonKinesis kinesisClient = getKinesisClient();
        Gson gson =  new GsonBuilder().setPrettyPrinting().create();
        List<PutRecordsRequestEntry> entries = new ArrayList<>();
        PutRecordsRequest putRecordsRequest = new PutRecordsRequest();
        putRecordsRequest.setStreamName(kinesis_topic);
        while (true){

            try {
                    JsonObject object = queue.take();
                    PutRecordsRequestEntry entry = new PutRecordsRequestEntry();
                    entry.setData(ByteBuffer.wrap(gson.toJson(object).getBytes()));
                    entry.setPartitionKey(UUID.randomUUID().toString());
                    entries.add(entry);


                if(entries.size() == record_batch_threshold){
                        Instant start = Instant.now();
                        logger.debug("time before "+"--"+start);
                        sendDataAndClearEntry(kinesisClient, entries, putRecordsRequest);
                        Instant end = Instant.now();
                        Duration timeElapsed = Duration.between(start, end);
                        logger.debug("record sent = " + entries.size());
                        logger.debug("time after "+ end);
                        logger.debug("Time taken: "+ timeElapsed.toMillis() +" mseconds");
                        entries.clear();
                    }


              } catch (InterruptedException e) {
               logger.error(e.getMessage(),e);
            }
        }
    }

    private void sendDataAndClearEntry(AmazonKinesis kinesisClient, List<PutRecordsRequestEntry> entries, PutRecordsRequest putRecordsRequest) {
        try {
            putRecordsRequest.setRecords(entries);
            PutRecordsResult result = kinesisClient.putRecords(putRecordsRequest);

            Integer failedRecordCount = result.getFailedRecordCount();

            //logger.debug("failedRecordCount = " + failedRecordCount);
            for (PutRecordsResultEntry records : result.getRecords()){
                if (records.getErrorCode() != null){
                    logger.error("********Failed Record****************");
                    logger.error(records.getErrorCode());
                    logger.error(records.getErrorMessage());
                    logger.error(records.getSequenceNumber());
                    logger.error(records.getShardId());
                    logger.error("************************");
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }

    }

    private  AmazonKinesis getKinesisClient(){
        return AmazonKinesisClientBuilder
                .standard()
                .withRegion(region)
                .build();

    }
}
