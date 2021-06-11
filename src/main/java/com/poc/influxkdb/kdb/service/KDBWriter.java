package com.poc.influxkdb.kdb.service;

import com.poc.influxkdb.kdb.connection.QConnectionFactory;
import com.poc.kdb.source.c;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

@Service
public class KDBWriter {

    Logger logger = LoggerFactory.getLogger(KDBWriter.class);

    @Value("${application.kdb.host}")
    private String kdbHost;

    @Value("${application.kdb.port}")
    private int kdbPort;

    public String write() {

        QConnectionFactory qConnectionFactory = new QConnectionFactory(kdbHost, kdbPort);
        c c = null;
        try {
            List<String> list = Arrays.asList("MSTF","DOGE","GOGLE","AMZ","MS","FB");
            c = qConnectionFactory.getQConnection();
            while(true) {
                String name = list.get(new Random().nextInt(list.size()));

                double price = new Random().nextDouble();
                long size = new Random().nextLong();
                Object[] row = { new c.Timespan(), name, price, size};
                c.k(".u.upd", "trade", row);
              //  logger.debug("writing----");
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


        return "Data successfully dumped in KDB..";
    }


}
