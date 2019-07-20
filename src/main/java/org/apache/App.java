package org.apache;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.schema.MessageType;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.schema.Types;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.statistics.BinaryStatistics;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.avro.AvroParquetWriter;
import java.io.IOException;
import java.util.Random;
import org.apache.avro.Schema;
import static org.apache.avro.SchemaBuilder.builder;
import org.apache.avro.generic.GenericData;
import static org.apache.parquet.avro.AvroParquetWriter.builder;

import static org.apache.parquet.column.Encoding.BIT_PACKED;
import static org.apache.parquet.column.Encoding.BYTE_STREAM_SPLIT;
import org.apache.parquet.hadoop.ParquetWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;

/**
 * Hello world!
 *
 */
public class App {

    private static String schema
            = "{ \"type\": \"record\", \"name\": \"asd\", \"doc\": \"something\", \"fields\":"
            + " [{\"name\": \"value\", \"type\": \"double\"}]}";

    public static boolean verify(ArrayList<Double> expected, ArrayList<Double> received) {
        if (expected.size() != received.size()) {
            System.out.println("Sizes do not match.");
            return false;
        }
        for (int i = 0; i < expected.size(); ++i) {
            if (!expected.get(i).equals(received.get(i))) {
                return false;
            }
        }
        return true;
    }

    public static ArrayList<Double> readAll(ParquetReader<GenericData.Record> reader) throws IOException {
        ArrayList<Double> data = new ArrayList<Double>();
        GenericData.Record record;
        int cnt = 0;
        while ((record = reader.read()) != null) {
            data.add((Double) record.get("value"));
        }
        return data;
    }

    public static void tryCase(ArrayList<Double> allData, Schema schema, String baseName, boolean useDict, boolean useByteStream, CompressionCodecName codec) throws IOException {
        String extra = "";
        if (codec == CompressionCodecName.UNCOMPRESSED) {
            extra += "_no_codec";
        }
        if (useDict) {
            extra += "_dict";
        }
        if (useByteStream) {
            extra += "_bs";
        }
        
        String fName = baseName + "_f64" + extra + ".parquet";
        if (Files.exists(new File(fName).toPath())) {
            return;
        }
        //Files.deleteIfExists(new File(fName).toPath());
        String fullName = baseName + "_f64" + extra + ".parquet";
        org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(fullName);
        ParquetWriter<GenericData.Record> writer = null;
        writer = AvroParquetWriter.
                <GenericData.Record>builder(path)
                .withRowGroupSize(ParquetWriter.DEFAULT_BLOCK_SIZE)
                .withPageSize(ParquetWriter.DEFAULT_PAGE_SIZE)
                .withSchema(schema)
                .withConf(new Configuration())
                .withCompressionCodec(codec)
                .withValidation(false)
                .withFPByteStreamSplitEncoding(useByteStream)
                .withDictionaryEncoding(useDict)
                .build();
        long startTime = System.currentTimeMillis();
        for (Double v : allData) {
            GenericData.Record record = new GenericData.Record(schema);
            record.put("value", v.doubleValue());
            writer.write(record);
        }
        writer.close();
        long stopTime = System.currentTimeMillis();
        System.out.println(fullName + ": " + (stopTime-startTime));

        // Now check that we can read the data.
        ParquetReader<GenericData.Record> reader = null;
        reader = AvroParquetReader
                .<GenericData.Record>builder(path)
                .withConf(new Configuration())
                .build();
        ArrayList<Double> receivedData = readAll(reader);
        if (!verify(allData, receivedData)) {
            System.out.println("Decompression failure.");
        }
    }

    public static void main(String[] args) throws IOException {

        /*String[] dataFiles = {
            "../dataset/32bit/obs_info.sp",
            "../dataset/32bit/msg_bt.sp",
            "../dataset/32bit/msg_lu.sp",
            "../dataset/32bit/msg_sp.sp",
            "../dataset/32bit/msg_sweep3d.sp",
            "../dataset/32bit/num_brain.sp",
            "../dataset/32bit/num_comet.sp",
            "../dataset/32bit/num_control.sp",
            "../dataset/32bit/num_plasma.sp",
            "../dataset/32bit/obs_error.sp",
            "../dataset/32bit/obs_info.sp",
            "../dataset/32bit/obs_spitzer.sp",
            "../dataset/32bit/obs_temp.sp",
        };*/
        String[] dataFiles = {
            "../dataset/64bit/msg_bt.dp",
            "../dataset/64bit/msg_sppm.dp",
            "../dataset/64bit/msg_sweep3d.dp",
            "../dataset/64bit/num_brain.dp",
            "../dataset/64bit/num_comet.dp",
            "../dataset/64bit/num_control.dp",
            "../dataset/64bit/num_plasma.dp",
            "../dataset/64bit/obs_error.dp",
            "../dataset/64bit/obs_info.dp",
            "../dataset/64bit/obs_spitzer.dp",
            "../dataset/64bit/obs_temp.dp",
        };

        for (String dataName : dataFiles) {
            // Read whole file
            ArrayList<Double> allData = new ArrayList<Double>();
            String baseName = dataName.split("/")[3];
            DataInputStream in = new DataInputStream(new FileInputStream(dataName));
            try {
                while (true) {
                    Double v = in.readDouble();
                    allData.add(v);
                }
            } catch (Exception ex) {
                System.out.println("Num elements: " + Integer.toString(allData.size()));
            } finally {
                in.close();
            }

            Schema.Parser parser = new Schema.Parser();
            Schema schema = parser.parse(App.schema);
            // First with no encoding
            tryCase(allData, schema, baseName, false, false, CompressionCodecName.UNCOMPRESSED);
            tryCase(allData, schema, baseName, false, false, CompressionCodecName.GZIP);
            tryCase(allData, schema, baseName, true, false, CompressionCodecName.GZIP);
            tryCase(allData, schema, baseName, false, true, CompressionCodecName.GZIP);
        }
    }
}
