package com.app;

import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.SystemOutRule;


import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;

import static org.junit.Assert.*;

public class ParquetUtilTest {

    @Test
    public void writeToParquetTest() throws IOException {

        Schema schema = ParquetUtil.parseSchema("src/test/resources/sample.avsc");
        File parquetFile = new File("src/test/resources/newFile.parquet");
        assertFalse(parquetFile.exists());
        ParquetUtil.writeToFile(schema, "src/test/resources/sample.csv", parquetFile.getPath());
        assertTrue(parquetFile.exists());
        assertNotNull(parquetFile);

        ParquetReader<GenericData.Record> parquetReader = AvroParquetReader
                .<GenericData.Record>builder(new Path(parquetFile.getPath()))
                .withConf(new Configuration())
                .build();

        Reader csvReadPath = Files.newBufferedReader(Paths.get("src/test/resources/sample.csv"));
        CSVReader csvReader = new CSVReaderBuilder(csvReadPath).withSkipLines(1).build();

        String[] csvNextRecord;
        GenericData.Record parquetNextRecord;


        while (((parquetNextRecord = parquetReader.read()) != null) && ((csvNextRecord = csvReader.readNext()) != null)) {

            assertEquals(csvNextRecord[0], parquetNextRecord.get(0).toString());
            assertEquals(csvNextRecord[1], parquetNextRecord.get(1).toString());
        }

    }

    @Test
    public void wrongPathTest() throws IllegalArgumentException {

        File parquetFile = new File("src/test/resources/notExistingPath.parquet");

    }

    @Test
    public void nullPathTest() throws IllegalArgumentException {

        File parquetFile = new File("");

    }

    @Rule
    public final SystemOutRule systemOutRule = new SystemOutRule().enableLog();

    @Test
    public void readParquetFile() {

        ParquetUtil.readFile("src/test/resources/fileForRead.parquet", 5);
        String expected = "{\"id\": 0, \"hotel_cluster\": \"a\"}\r\n" +
                "{\"id\": 1, \"hotel_cluster\": \"b\"}\r\n" +
                "{\"id\": 2, \"hotel_cluster\": \"c\"}\r\n" +
                "{\"id\": 3, \"hotel_cluster\": \"d\"}\r\n" +
                "{\"id\": 4, \"hotel_cluster\": \"e\"}\r\n";
        String actual = systemOutRule.getLog();
        assertEquals(expected, actual);
    }

}