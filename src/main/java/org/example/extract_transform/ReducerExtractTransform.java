package org.example.extract_transform;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.example.CountersEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class ReducerExtractTransform extends Reducer<AvroKey<GenericRecord>, AvroValue<GenericRecord>, AvroKey<GenericRecord>,NullWritable> {
    private static Logger log = LoggerFactory.getLogger(ReducerExtractTransform.class);
    private static final Schema schema = new Schema.Parser().parse(
            "{" +
                    "  \"type\": \"record\"," +
                    "  \"name\": \"ReviewBook\"," +
                    "  \"fields\": [" +
                    "    {\"name\" : \"id\", \"type\":\"string\"},"+
                    "    {\"name\" : \"reviewerID\" , \"type\": \"string\" , \"order\": \"ignore\"}," +
                    "    {\"name\" : \"asin\" ,\"type\": \"string\" }," +
                    "    {\"name\" : \"reviewerName\", \"type\": \"string\", \"order\": \"ignore\"}," +
                    "    {\"name\" : \"adjectiveWord\" ,\"type\": {\"type\": \"array\",\"items\": \"string\"},\"order\": \"ignore\"}," +
                    "    {\"name\" :  \"reviewText\" , \"type\": \"string\", \"order\" :  \"ignore\"}" +
                    "  ]" +
                    "}"
    );

    @Override
    protected void reduce(AvroKey<GenericRecord> key, Iterable<AvroValue<GenericRecord>> values, Reducer<AvroKey<GenericRecord>, AvroValue<GenericRecord>, AvroKey<GenericRecord>, NullWritable>.Context context) throws IOException, InterruptedException {
        for(AvroValue<GenericRecord> value : values){
            context.getCounter(CountersEnum.TOTAL_MAP_RECORDS).increment(1);
            GenericRecord record = value.datum();
            GenericRecord newRecord = new GenericData.Record(schema);
            newRecord.put("id",  String.valueOf(context.getCounter(CountersEnum.TOTAL_MAP_RECORDS).getValue()));
            newRecord.put("reviewerID",record.get("reviewerID"));
            newRecord.put("asin",record.get("asin"));
            newRecord.put("reviewerName",record.get("reviewerName"));
            newRecord.put("adjectiveWord",record.get("adjectiveWord"));
            newRecord.put("reviewText",record.get("reviewText"));


            context.write(new AvroKey<>(newRecord),NullWritable.get());
        }
    }
}
