package com.poc.influxkdb.kdb.connection;

import com.poc.kdb.source.c;

import java.io.IOException;

public class QConnectionFactory {
    private final String host;
    private final int port;

    public QConnectionFactory(String host, int port) {
        this.host=host;
        this.port=port;
    }
    public c getQConnection() throws c.KException, IOException {
        return new c(host,port);
    }

    public static QConnectionFactory getDefault() {
        return new QConnectionFactory("localhost", 5001);
    }
}
