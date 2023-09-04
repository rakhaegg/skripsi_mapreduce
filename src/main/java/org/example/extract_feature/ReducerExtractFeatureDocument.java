package org.example.extract_feature;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;


public class ReducerExtractFeatureDocument extends Reducer<AvroKey<String>, AvroValue<Long>, AvroKey<GenericRecord>, NullWritable> {
    private static Logger logger = LoggerFactory.getLogger(ReducerExtractFeatureDocument.class);
    private static final Schema schema2 = new Schema.Parser().parse(
            "{" +
                    "  \"type\": \"record\"," +
                    "  \"name\": \"DocumentFrequency\"," +
                    "  \"fields\": [" +
                    "    {\"name\" : \"word\" , \"type\": \"string\"}," +
                    "    {\"name\" : \"idf\" , \"type\" :  \"double\"}" +
                    "  ]" +
                    "}"
    );
    private long lengthDocument = 0;
    @Override
    protected void setup(Reducer<AvroKey<String>, AvroValue<Long>, AvroKey<GenericRecord>, NullWritable>.Context context) throws IOException, InterruptedException {
        FileSystem fs = FileSystem.get(context.getConfiguration());
        URI[] as = context.getCacheFiles();
        Path patLengthDocument = new Path(as[0]);
        try(BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fs.open(patLengthDocument)))){
            String line;
            while ((line = bufferedReader.readLine()) != null){
                this.lengthDocument =  Long.parseLong(line);
            }
        }

    }

    @Override
    protected void reduce(AvroKey<String> key, Iterable<AvroValue<Long>> values, Reducer<AvroKey<String>, AvroValue<Long>, AvroKey<GenericRecord>, NullWritable>.Context context) throws IOException, InterruptedException {
        long count = 0L;
        for(AvroValue<Long> value: values){
            count = count + value.datum();
        }
        GenericData.Record avroRecordForCalculateDocumentFrequency = new GenericData.Record(schema2);
        avroRecordForCalculateDocumentFrequency.put("word",key.datum());
        avroRecordForCalculateDocumentFrequency.put("idf",Math.log(((double) lengthDocument+1)/(count+1)) +1);
       try {
            context.write(new AvroKey<>(avroRecordForCalculateDocumentFrequency),NullWritable.get());
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
