package org.apache;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.avro.AvroParquetWriter;
import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.parquet.hadoop.ParquetWriter;
import java.nio.file.Files;
import java.util.ArrayList;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;

/**
 * Hello world!
 *
 */
public class App {

    private static String schema64
            = "{ \"type\": \"record\", \"name\": \"asd\", \"doc\": \"something\", \"fields\":"
            + " [{\"name\": \"value\", \"type\": \"double\"}]}";

    public static <T> boolean verify(ArrayList<T> expected, ArrayList<T> received) {
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

    public static <T> ArrayList<T> readAll(ParquetReader<GenericData.Record> reader) throws IOException {
        ArrayList<T> data = new ArrayList<T>();
        GenericData.Record record;
        int cnt = 0;
        while ((record = reader.read()) != null) {
            data.add((T) record.get("value"));
        }
        return data;
    }
    
    public static String genName(String baseName, boolean useDict, boolean useByteStream, CompressionCodecName codec, boolean isF32) {
        String name = baseName;
        switch(codec) {
            case UNCOMPRESSED:
                name += "_no_codec";
                break;
            case SNAPPY:
                name += "_snappy";
                break;
            case GZIP:
                name += "_gzip";
                break;
            case ZSTD:
                name += "_zstd";
                break;
            case BROTLI:
                name += "_brotli";
                break;
           case LZO:
                name += "_brotli";
                break;
           default:
        }
        if (useByteStream) {
            name += "_bs";
        }
        if (isF32) {
            name += "_f32";
        } else {
            name += "_f64";
        }
        name += ".parquet";
        return name;
    }

    public static <T> void tryCase(ArrayList<T> allData, Schema schema, String baseName, boolean useDict, boolean useByteStream, CompressionCodecName codec, boolean isf32) throws IOException {
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
        
        String fName = genName(baseName, useDict, useByteStream, codec, isf32);
        if (Files.exists(new File(fName).toPath())) {
            return;
        }
        org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(fName);
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
        for (T v : allData) {
            GenericData.Record record = new GenericData.Record(schema);
            record.put("value", v);
            writer.write(record);
        }
        writer.close();
        long stopTime = System.currentTimeMillis();
        System.out.println(fName + ": " + (stopTime-startTime));

        // Now check that we can read the data.
        ParquetReader<GenericData.Record> reader = null;
        reader = AvroParquetReader
                .<GenericData.Record>builder(path)
                .withConf(new Configuration())
                .build();
        ArrayList<T> receivedData = readAll(reader);
        if (!verify(allData, receivedData)) {
            System.out.println("Decompression failure.");
        }
    }

    public static void main(String[] args) throws IOException {

        
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
            Schema schema = parser.parse(App.schema64);
            // First with no encoding
            tryCase(allData, schema, baseName, false, false, CompressionCodecName.BROTLI, false);
            tryCase(allData, schema, baseName, false, true, CompressionCodecName.BROTLI, false);
            tryCase(allData, schema, baseName, false, false, CompressionCodecName.ZSTD, false);
            tryCase(allData, schema, baseName, false, true, CompressionCodecName.ZSTD, false);
        }
        
        String[] dataFilesf32 = {
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
        };
        
        for (String dataName : dataFilesf32) {
            // Read whole file
            ArrayList<Float> allData = new ArrayList<Float>();
            String baseName = dataName.split("/")[3];
            DataInputStream in = new DataInputStream(new FileInputStream(dataName));
            try {
                while (true) {
                    Float v = in.readFloat();
                    allData.add(v);
                }
            } catch (Exception ex) {
                System.out.println("Num elements: " + Integer.toString(allData.size()));
            } finally {
                in.close();
            }

            Schema.Parser parser = new Schema.Parser();
            Schema schema = parser.parse(App.schema64);
            // First with no encoding
            tryCase(allData, schema, baseName, false, false, CompressionCodecName.ZSTD, true);
            tryCase(allData, schema, baseName, false, true, CompressionCodecName.ZSTD, true);
        }
    }
}
