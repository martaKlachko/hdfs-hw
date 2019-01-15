package com.app;


public class Main {

    public static void main(String[] args) {

//        Schema schema = ParquetUtil.parseSchema(args[0]);
//
//        ParquetUtil.writeToFile(schema,args[1],args[2]);
//
//        ParquetUtil.readFile(args[2],Integer.parseInt(args[3]));
        ParquetUtil.readFile("src/test/resources/fileForRead.parquet", 20);
    }

}