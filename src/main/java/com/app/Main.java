package com.app;

import org.apache.avro.Schema;

public class Main {

    public static void main(String[] args) {

        Schema schema = ParquetUtils.parseSchema(args[0]);

        ParquetUtils.writeToParquet(schema,args[1],args[2]);

        ParquetUtils.readParquetFile(args[2],Integer.parseInt(args[3]));

//        ParquetUtils.readParquetFile("src/test/resources/fileForRead.parquet",5);
    }

}