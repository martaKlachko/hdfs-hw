package com.app;

import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;

public class ParquetUtil {

    public static Schema parseSchema(String path) {
        Schema.Parser parser = new Schema.Parser();
        Schema schema = null;

        try {
            schema = parser.parse(new File(path));

        } catch (Exception e) {
            e.printStackTrace();
        }

        return schema;
    }

    public static void writeToFile(Schema schema, String pathToCsv, String parquetPath) {

        Path path = new Path(parquetPath);
        ParquetWriter<GenericData.Record> writer = null;
        GenericData.Record record;

        try (
                Reader reader = Files.newBufferedReader(Paths.get(pathToCsv));
                CSVReader csvReader = new CSVReaderBuilder(reader).withSkipLines(1).build();
        ) {


            writer = AvroParquetWriter.
                    <GenericData.Record>builder(path)
                    .withConf(new Configuration())
                    .withPageSize(ParquetWriter.DEFAULT_PAGE_SIZE)
                    .withRowGroupSize(ParquetWriter.DEFAULT_BLOCK_SIZE)
                    .withSchema(schema)
                    .withValidation(false)
                    .withDictionaryEncoding(false)
                    .withCompressionCodec(CompressionCodecName.SNAPPY)
                    .build();

            String[] nextRecord;

            while ((nextRecord = csvReader.readNext()) != null) {
                record = new GenericData.Record(schema);

                for (int i = 0; i < nextRecord.length; i++) {

                    Schema.Type type = schema.getFields().get(i).schema().getType();

                    boolean isNumber = NumberUtils.isNumber(nextRecord[i]);

                    if (type == Schema.Type.INT) {
                        int putRecord = isNumber ? Integer.parseInt(nextRecord[i]) : 0;
                        record.put(i, putRecord);
                    }

                    if (type == Schema.Type.DOUBLE) {
                        double putRecord = isNumber ? Double.parseDouble(nextRecord[i]) : 0.0d;
                        record.put(i, putRecord);
                    }

                    if (type == Schema.Type.LONG) {
                        long putRecord = isNumber ? Long.parseLong(nextRecord[i]) : 0L;
                        record.put(i, putRecord);
                    }

                    if (type == Schema.Type.STRING) {
                        record.put(i, nextRecord[i]);
                    }
                }
                writer.write(record);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (writer != null) {
                try {
                    writer.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static void readFile(String parquetPath, int maxRows) {

        Path path = new Path(parquetPath);
        try (ParquetReader<GenericData.Record> reader = AvroParquetReader
                .<GenericData.Record>builder(path)
                .withConf(new Configuration())
                .build()) {

            GenericData.Record record;

            int k = maxRows;
            while (((record = reader.read()) != null) && k > 0) {
                System.out.println(record);
                k--;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}