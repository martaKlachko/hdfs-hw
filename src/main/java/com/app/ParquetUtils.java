package com.app;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;

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

public class ParquetUtils {

    public static Schema parseSchema(String schemaPath) {
        Schema.Parser parser = new Schema.Parser();
        Schema schema = null;

        try {
            schema = parser.parse(new File(schemaPath));

        } catch (IOException e) {
            e.printStackTrace();
        }

        return schema;
    }

    public static void writeToParquet(Schema schema, String csvPath, String parquetPath) {

        Path path = new Path(parquetPath);
        ParquetWriter<GenericData.Record> writer = null;
        GenericData.Record record;

        try (
                Reader reader = Files.newBufferedReader(Paths.get(csvPath));
                CSVReader csvReader = new CSVReaderBuilder(reader).withSkipLines(1).build();
        ) {
            String[] nextRecord;

            writer = AvroParquetWriter.
                    <GenericData.Record>builder(path)
                    .withRowGroupSize(ParquetWriter.DEFAULT_BLOCK_SIZE)
                    .withPageSize(ParquetWriter.DEFAULT_PAGE_SIZE)
                    .withSchema(schema)
                    .withConf(new Configuration())
                    .withCompressionCodec(CompressionCodecName.SNAPPY)
                    .withValidation(false)
                    .withDictionaryEncoding(false)
                    .build();

            while ((nextRecord = csvReader.readNext()) != null) {
                record = new GenericData.Record(schema);

                for (int i=0; i<nextRecord.length; i++) {

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
                        record.put(i,nextRecord[i]);
                    }
                }
                writer.write(record);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if(writer != null) {
                try {
                    writer.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static void readParquetFile(String parquetFilePath, int maxRows) {
        ParquetReader<GenericData.Record> reader = null;

        Path path = new Path(parquetFilePath);
        try {
            reader = AvroParquetReader
                    .<GenericData.Record>builder(path)
                    .withConf(new Configuration())
                    .build();
            GenericData.Record record;

            int i = maxRows;
            while ( ((record = reader.read()) != null) && i>0 ) {
                System.out.println(record);
                i--;
            }
        }catch(IOException e) {
            e.printStackTrace();
        }finally {
            if(reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

}