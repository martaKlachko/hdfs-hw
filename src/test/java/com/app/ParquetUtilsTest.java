package com.app;

import org.apache.avro.Schema;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.SystemOutRule;

import java.io.File;

import static org.junit.Assert.*;

public class ParquetUtilsTest {

    @Test
    public void writeToParquet() {

        Schema schema = ParquetUtils.parseSchema("src/test/resources/sample.avsc");
        File parquetFile = new File("src/test/resources/fileToBeCreated.parquet");
        assertFalse(parquetFile.exists());
        ParquetUtils.writeToParquet(schema,"src/test/resources/sample.csv",parquetFile.getPath());
        assertTrue(parquetFile.exists());
    }

    @Rule
    public final SystemOutRule systemOutRule = new SystemOutRule().enableLog();

    @Test
    public void readParquetFile() {

        ParquetUtils.readParquetFile("src/test/resources/fileForRead.parquet",1);

        //TOD DO
//        assertEquals(  "{\"id\": 0, \"hotel_cluster\": \"a\"}", systemOutRule.getLog());
    }

}