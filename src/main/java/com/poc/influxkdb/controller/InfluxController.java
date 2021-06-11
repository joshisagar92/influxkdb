package com.poc.influxkdb.controller;


import com.poc.influxkdb.influxstream.service.InfluxReadingService;
import com.poc.influxkdb.influxstream.service.InfluxWriterService;
import com.poc.influxkdb.kdb.service.KDBWriter;
import com.poc.influxkdb.kdb.service.KdbReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/")
public class InfluxController {

    Logger logger = LoggerFactory.getLogger(InfluxController.class);

    @Value("${application.influx.token}")
    private String strToken;

    private char[] token;

    @Value("${application.influx.bucket}")
    private String bucket;

    @Value("${application.influx.org}")
    private String org;

    @Value("${application.influx.url}")
    private String influx_url;

    @Autowired
    InfluxWriterService writerService;

    @Autowired
    InfluxReadingService readingService;

    @Autowired
    KDBWriter kdbWriter;

    @Autowired
    KdbReader kdbReader;

    @PostMapping("/dump")
    public String dumpData(){
        logger.debug(strToken);
        logger.debug(bucket);
        logger.debug(org);
        logger.debug(influx_url);
        logger.debug("---------------------------------------------------------");
        String result = writerService.writeData(strToken, bucket, org, "http://"+influx_url);
        return result;
    }

    @PostMapping("/send")
    public String sendData(){
        logger.debug(strToken);
        logger.debug(bucket);
        logger.debug(org);
        logger.debug(influx_url);
        logger.debug("---------------------------------------------------------");
        readingService.startReading(strToken,org,"http://"+influx_url);

        return "Reading successfully started";
    }


    @PostMapping("/kdb/dump")
    public String dumpDataFomKdb(){
        String result = kdbWriter.write();
        return result;
    }

    @PostMapping("/kdb/dumpandsend")
    public String sendDataFomKdb(){
        String result = kdbReader.send();
        return result;
    }


}
