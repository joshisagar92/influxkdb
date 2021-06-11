package com.poc.influxkdb.influxstream.service;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.WriteApi;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.Instant;

@Service
public class InfluxWriterService {


    Logger logger = LoggerFactory.getLogger(InfluxWriterService.class);

    @Value("${application.influx.host}")
    private String host;


    public String writeData(String strToken, String bucket, String org, String influx_url) {
        try {
            char[] token = strToken.toCharArray();
            logger.debug(token.toString());

            InfluxDBClient influxDBClient =
                    InfluxDBClientFactory.create(influx_url, token, org, bucket);

            try (WriteApi writeApi = influxDBClient.getWriteApi()) {
                for (int i = 0; i < 10; i++) {
                    double random = Math.random();
                    logger.debug(i+"--- "+random);
                    System.out.println(random);
                    Point point = Point.measurement("x-poc-data")
                            .addTag("host", host)
                            .addField("used_percent", random)
                            .time(Instant.now().toEpochMilli(), WritePrecision.MS);
                    writeApi.writePoint(point);
                }

            }catch (Exception e){
                return "Error during writing the data" + e.getMessage();
            }
        } catch (Exception e) {
            return "Error during connecting to influxdb" + e.getMessage();
        }

        return "Data successfully dumped";
    }
}
