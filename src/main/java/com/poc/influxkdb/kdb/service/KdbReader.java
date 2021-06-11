package com.poc.influxkdb.kdb.service;


import com.google.gson.JsonObject;
import com.poc.influxkdb.influxstream.repository.KinesisRepository;
import com.poc.influxkdb.kdb.connection.QConnectionFactory;
import com.poc.kdb.source.c;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Service
public class KdbReader {

    private final Logger logger = LoggerFactory.getLogger(KdbReader.class);
    private final BlockingQueue<JsonObject> queue = new ArrayBlockingQueue<>(200);

    @Value("${application.kdb.host}")
    private String kdbHost;

    @Value("${application.kdb.port}")
    private int kdbPort;

    private final ExecutorService executorService = Executors.newFixedThreadPool(3);

    @Autowired
    KinesisRepository kinesisRepository;

    QConnectionFactory qConnectionFactory = null;
    public String send() {
        logger.debug("---------------");
        logger.debug(kdbHost);
        logger.debug(String.valueOf(kdbPort));
        logger.debug("---------------");
        qConnectionFactory = new QConnectionFactory(kdbHost, kdbPort);

        /*executorService.submit(() -> {
             writingData();
        });*/

        List<String> response = new ArrayList<>();
        executorService.submit(() -> {
            String result = startReadingFromKDB();
            response.add(result);
        });

        executorService.submit(() -> {
             kinesisRepository.send(queue);
        });

        return response.size() > 0?response.get(0):"Started successfully ...." ;
    }

   /* private void writingData() {
        c c = null;
        try {
            List<String> list = Arrays.asList("MSTF","DOGE","GOGLE","AMZ","MS","FB");
            c = qConnectionFactory.getQConnection();
            for (int i = 0; i < 500; i++) {
                String name = list.get(new Random().nextInt(list.size()));

                double price = new Random().nextDouble();
                long size = new Random().nextInt(1000);
                Object[] row = { new c.Timespan(), name, price, size};
                c.k(".u.upd", "trade", row);
            }

        } catch (com.poc.kdb.source.c.KException e) {
            logger.error(e.getMessage(),e);
        } catch (IOException e) {
            logger.error(e.getMessage(),e);
        }finally {
            try {
                c.close();
            } catch (IOException e) {
                logger.error(e.getMessage(),e);
            }
        }
    }*/

    @NotNull
    private String startReadingFromKDB() {

        c c = null;
        try {
            c = qConnectionFactory.getQConnection();
            Object[] responses = (Object[]) c.k(".u.sub[`trade;`]");

            c.Flip table = (c.Flip) responses[1];
            addDataToQueue(table);
            while (true) {
                logger.debug("Waiting-------");
                Object response = c.k();
                logger.debug("done-------");
                if(response != null) {
                    Object[] data = (Object[]) response;
                    c.Flip tables = (c.Flip) data[2];
                    addDataToQueue(tables);
                }
            }

        } catch (com.poc.kdb.source.c.KException e) {
            logger.error(e.getMessage(),e);
            c.close();
            return "READING KDB : Error while reading the data"+e.getMessage();
        } catch (IOException e) {
            c.close();
            logger.error(e.getMessage(),e);
            return "READING KDB : Error while reading the data"+e.getMessage();

        }finally {

            return "READING KDB : Reading Started Successfully";
        }
    }

    private void addDataToQueue(c.Flip table) throws InterruptedException {
        String[] columnNames = table.x;
        Object[] data = table.y;
        JsonObject jsonObject = new JsonObject();;
        for (int j=0; j<columnNames.length;j++){
            addProperty(columnNames[j],jsonObject, data[j]);
        }
        queue.put(jsonObject);

    }

    private static void addProperty(String name, JsonObject jsonObject, Object o) {
        if(o instanceof Float[]){
              jsonObject.addProperty(name,Arrays.toString((Float[]) o));
        }else if(o instanceof Double[]){
            jsonObject.addProperty(name,Arrays.toString((Double[]) o));
        }else if(o instanceof Long[]){
             jsonObject.addProperty(name,Arrays.toString((Long[]) o));
        }else if(o instanceof Integer[]){
            jsonObject.addProperty(name,Arrays.toString((Integer[]) o));
        }else if(o instanceof String[]){
            jsonObject.addProperty(name,Arrays.toString((String[]) o));
        }else if(o instanceof float[]){
            jsonObject.addProperty(name,Arrays.toString((float[]) o));
        }else if(o instanceof double[]){
            jsonObject.addProperty(name,Arrays.toString((double[]) o));
        }else if(o instanceof long[]){
            jsonObject.addProperty(name,Arrays.toString((long[]) o));
        }else if(o instanceof int[]){
            jsonObject.addProperty(name,Arrays.toString((int[]) o));
        }else if(o instanceof String){
            jsonObject.addProperty(name,(String) o);
        }else if(o instanceof c.Timespan[]){
            jsonObject.addProperty(name,Arrays.toString((c.Timespan[]) o));
        }else {
            jsonObject.addProperty(name,String.valueOf(o));
        }
    }
}
