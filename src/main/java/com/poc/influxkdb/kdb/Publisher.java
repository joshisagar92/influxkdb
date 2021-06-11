package com.poc.influxkdb.kdb;


import com.poc.kdb.source.c;

import java.io.IOException;

public class Publisher {


    public static void main(String[] args) throws IOException, c.KException {
        c c = null;
        try {
            System.out.println("Try to connect to tickerplant");
            c = new c("localhost", 5000);
            System.out.println("Connected to tickerplant");

            Object[] row = { new c.Timespan(), "MSFT", 174.57, 300L };

            System.out.println("Try to insert record to 'trade' table");
            c.k(".u.upd", "trade", row);

            System.out.println("Record inserted to 'trade' table");




        } catch (Exception e) {
            throw e;
        } finally {
            c.close();
        }

    }



}
