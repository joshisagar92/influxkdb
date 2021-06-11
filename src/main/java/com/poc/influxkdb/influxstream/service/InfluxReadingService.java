package com.poc.influxkdb.influxstream.service;


import com.google.gson.JsonObject;
import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.QueryApi;
import com.influxdb.query.FluxRecord;
import com.influxdb.query.FluxTable;
import com.poc.influxkdb.influxstream.repository.KinesisRepository;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

@Service
public class InfluxReadingService {

    @Value("${application.timestampfile.directory}")
    private String directory;
    @Value("${application.timestampfile.folder}")
    private String folder;

    private final String fileName = "timestamp.txt";
    Logger logger = LoggerFactory.getLogger(InfluxReadingService.class);

    private final Map<String, Instant> configuration = new ConcurrentHashMap<>();

    private final BlockingQueue<JsonObject> queue = new ArrayBlockingQueue<>(200);

    private final ExecutorService executorService = Executors.newFixedThreadPool(2);

    @Value("${application.influx.host}")
    private String host;


    @Autowired
    private KinesisRepository kinesisRepository;

    public void startReading(String strToken, String org, String influx_url) {

        char[] token = strToken.toCharArray();
        InfluxDBClient influxDBClient;
        try {
            influxDBClient = InfluxDBClientFactory
                    .create(influx_url, token, org);
        } catch (Exception e) {
            throw new RuntimeException("Error during connection to influxdb "+influx_url+"-"+org+"-"+token);
        }

        executorService.submit(() -> {
             readFromInflux(influxDBClient);
        });

        executorService.submit(() -> {
            kinesisRepository.send(queue);
        });

    }

    private Object readFromInflux(InfluxDBClient influxDBClient) {
        String timeStamp = getTimeStampFromFile();
        if(!"".equalsIgnoreCase(timeStamp)){
            configureLastTimeStamp(Instant.parse(timeStamp.trim()));
        }

        //TODO: SAFELY GET OUT FROM THREAD.START NEW THREAD
        QueryApi queryApi = influxDBClient.getQueryApi();
        while (true){
            String flux = getQuery();
            try {
                List<FluxTable> tables = queryApi.query(flux);
                for (FluxTable fluxTable : tables) {
                    List<FluxRecord> records = fluxTable.getRecords();
                    for (FluxRecord fluxRecord : records) {
                        configureLastTimeStamp(fluxRecord.getTime());

                        JsonObject jsonObject = new JsonObject();
                        jsonObject.addProperty("time",fluxRecord.getTime().toString());
                        jsonObject.addProperty("used_memory",fluxRecord.getValueByKey("_value").toString());
                        queue.put(jsonObject);
                        System.out.println("--------------------------------------------------------------");
                        System.out.println(fluxRecord.getTime() + ": " + fluxRecord.getValueByKey("_value"));
                        System.out.println("--------------------------------------------------------------");
                    }
                }
            } catch (Exception e) {
                logger.error(e.getMessage(),e);
                saveTimeStampToDisk();
            }
        }
    }

    protected void configureLastTimeStamp(Instant time) {
        configuration.put("last_time",time);
    }

    protected String getTimeStampFromFile() {

        File file = new File(getPath());
        logger.debug("File Found : " + file.exists());

        String content = "";
        if(!file.exists()){
            return content;
        }

        try {
            content = new String(Files.readAllBytes(file.toPath()));
        } catch (IOException e) {
            logger.error(e.getMessage(),e);
        }
        return content;
    }

    protected void saveTimeStampToDisk() {


        URL resource = this.getClass().getResource("/");
        FileWriter fileWriter = null;
        try {
            File file = new File(getPath());
            file.createNewFile();
            fileWriter = new FileWriter(file);

            fileWriter.write(String.valueOf(configuration.get("last_time")));
        } catch (IOException e) {
            logger.error(e.getMessage(),e);
        }finally {
            try {
                fileWriter.close();
            } catch (IOException e) {
                logger.error(e.getMessage(),e);
            }
        }
    }

    @NotNull
    protected String getPath() {

        String path = "";
        if(directory != null  && !"".equals(directory)){
            path += directory +File.separator;
        }
        if(folder != null  && !"".equals(folder)){
            path += folder+File.separator;
        }

        path +=this.fileName;

        logger.debug("PATH --- "+path);
        return path;
    }

    @NotNull
    private String getQuery() {
        if(configuration.containsKey("last_time")){
            return "from(bucket: \"influx_bucket\")\n" +
                    "  |> range(start: 0)\n" +
                    "  |> filter(fn: (r) => r[\"_time\"] > "+configuration.get("last_time")+")\n" +
                    "  |> filter(fn: (r) => r[\"_measurement\"] == \"x-poc-data\")\n" +
                    "  |> filter(fn: (r) => r[\"_field\"] == \"used_percent\")\n" +
                    "  |> filter(fn: (r) => r[\"host\"] == \""+host.toString()+"\")\n";
        }else{
            return "from(bucket: \"influx_bucket\")\n" +
                    "  |> range(start: 0)\n" +
                    "  |> filter(fn: (r) => r[\"_measurement\"] == \"x-poc-data\")\n" +
                    "  |> filter(fn: (r) => r[\"_field\"] == \"used_percent\")\n" +
                    "  |> filter(fn: (r) => r[\"host\"] == \""+host.toString()+"\")\n";
        }

    }
}
