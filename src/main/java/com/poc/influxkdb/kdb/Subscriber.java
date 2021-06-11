package com.poc.kdb;

import com.poc.kdb.source.c;

import java.io.IOException;

public class Subscriber {


    public static void main(String[] args) {
        c c = null;
        try {
            c = new c("localhost", 5000);

            Object[] responses = (Object[]) c.k(".u.sub[`trade;`]");


            //to extract more than one table
//            Object[] response = (Object[]) qConnection.k(".u.sub[`;`]");


            System.out.println("table name: " + responses[0]);

            c.Flip table = (c.Flip) responses[1];

            String[] columnNames = table.x;
            for (int i = 0; i < columnNames.length; i++) {
                System.out.printf("Column %d is named %s\n", i, columnNames[i]);
            }


           /* while (true) {

                //wait on k()
                System.out.println("Waiting-------");
                Object response = c.k();
                System.out.println("printing-------");
                if(response != null) {
                    Object[] data = (Object[]) response;
                    c.Flip tables = (c.Flip) data[2];

                    String[] columnNamess = tables.x;

                    Object[] result = tables.y;
                    for (int i = 0; i < result.length; i++) {
                        System.out.printf("Column %d is named %s\n", i, result[i]);
                    }

                }
            }*/



        } catch (com.poc.kdb.source.c.KException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
