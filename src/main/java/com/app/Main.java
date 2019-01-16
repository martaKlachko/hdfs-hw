package com.app;


import org.apache.avro.Schema;

public class Main {

    public static void main(String[] args) {
        //reading schema
        Schema schema = ParquetUtil.parseSchema(args[0]);
        //converting csv file (args[1]) to parquet file(args[2]) using schema(args[0])
        ParquetUtil.writeToFile(schema, args[1], args[2]);
        //printing content of created earlier parquet file to console
        ParquetUtil.readFile(args[2], Integer.parseInt(args[3]));


        //  ParquetUtil.readFile("src/test/resources/fileForRead.parquet", 20);

    }

}
