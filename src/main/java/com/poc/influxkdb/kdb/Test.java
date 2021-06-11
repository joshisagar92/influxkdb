package com.poc.kdb;

import com.poc.influxkdb.kdb.connection.QConnectionFactory;
import com.poc.kdb.source.c;

import java.io.IOException;
import java.util.Arrays;

public class Test {

    public static void main(String[] args) throws IOException, c.KException {

        c qConnection = QConnectionFactory.getDefault().getQConnection();


        /*//Create rows and columns
        int[] values = {1, 2, 3};
        Object[] data = new Object[] {values};
        String[] columnNames = new String[] {"column"};
        //Wrap values in dictionary
        c.Dict dict = new c.Dict(columnNames, data);
        //Create table using dict
        c.Flip table = new c.Flip(dict);
        //Send to q using 'insert' method
        qConnection.ks("insert", "t1", table);*/



        c.Flip flip = (c.Flip) qConnection.k("select from trade where sym = `a");

        //Retrieve columns and data
        String[] columnNames = flip.x;
        Object[] columnData = flip.y;


        //Extract row data into typed arrays

        java.sql.Time[] time = (java.sql.Time[]) columnData[0];
        String[] sym = (String[]) columnData[1];
        double[] price = (double[]) columnData[2];
        int[] size = (int[]) columnData[3];
        int rows = time.length;


        for (String columnName : columnNames)
        {
            System.out.print(columnName + "\t\t\t");
        }
        System.out.println("\n-----------------------------------------------------");

        for (int i = 0; i < rows; i++)
        {
            System.out.print(time[i]+"\t"+sym[i]+"\t\t\t"+price[i]+"\t\t\t"+size[i]+"\n");
        }


        /*Object result = qConnection.k("{x+y}",4,3);
        System.out.println(result.toString());


        //asynchronous assignment of function
        qConnection.ks("jFunc:{x-y+z}");

        //synchronous calling of that function
        result = qConnection.k("jFunc",10,4,3);
        System.out.println(result);



        // Start by casting the returned Object into Object[]
        Object[] resultArray = (Object[]) qConnection.k("((1 2 3 4); (1 2))");


        c.Dict dict = (c.Dict) qConnection.k("`a`b`c!((1 2 3);\"Second\"; (`x`y`z))");


        //Create keys and values
        Object[] keys = {"a", "b", "c"};
        int[] values = {100, 200, 300};
        //Set in dict constructor
        c.Dict dict = new c.Dict(keys, values);
        //Set in q session
        qConnection.k("set","dict",dict);

        //Retrieve keys from dictionary

        String[] keys = (String[]) dict.x;

        System.out.println(Arrays.toString(keys));

        //Retrieve values
        Object[] values = (Object[]) dict.y;

        //System.out.println(Arrays.toString(values));
        System.out.println(Arrays.toString((long[])values[0]));
        System.out.println(Arrays.toString((char[]) values[1]));
        //System.out.println(Arrays.toString(values));

        //These can then be worked with similarly to nested lists*/



        //initiate reconnect loop (possibly within a catch block).
        /*while (true) {
            try {
                System.err.println("Connection failed - retrying..");
                //Wait a bit before trying to reconnect
                Thread.sleep(5000);
                qConnection = qConnFactory.getQConnection();
                System.out.println("Connection re-established! Resuming..");
                //Exit loop
                break;
            } catch (IOException | KException e1) {
                //resume loop if it fails
                continue;
            }
  …
        }*/



        qConnection.close();

    }


}
