package org.example.extract_transform;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MapperExtractTransform extends Mapper<LongWritable,Text, AvroKey<GenericRecord>, AvroValue<GenericRecord>> {
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
    private Set<String> adjWord = new HashSet<>();
    private Set<String> stopWordList = new HashSet<>();
    private static Logger log = LoggerFactory.getLogger(MapperExtractTransform.class);
    @Override
    protected void setup(Mapper<LongWritable, Text,AvroKey<GenericRecord>, AvroValue<GenericRecord>>.Context context) throws IOException, InterruptedException {
        URI[] uris = context.getCacheFiles();

        FileSystem fs = FileSystem.get(context.getConfiguration());
        try (BufferedReader bf = new BufferedReader(new InputStreamReader(fs.open(new Path(uris[0].toString()))))) {
            String _item_word = "";
            while ((_item_word = bf.readLine()) != null)
                this.adjWord.add(_item_word);
        }
        try (BufferedReader bf = new BufferedReader(new InputStreamReader(fs.open(new Path(uris[1].toString()))))) {
            String _item_word = "";
            while ((_item_word = bf.readLine()) != null)
                stopWordList.add(_item_word);
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, AvroKey<GenericRecord>, AvroValue<GenericRecord>>.Context context) throws IOException, InterruptedException {
        if (!value.toString().contains("review/text")) {
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode jsonNode = objectMapper.readTree(value.toString());
            if (jsonNode.has("reviewText")
                && jsonNode.has("reviewerID") &&
                    jsonNode.has("asin") &&
                    jsonNode.has("reviewerName")) {
                String reviewText = jsonNode.get("reviewText").asText();
                String lowerData =reviewText.toLowerCase();
                String regex = "(?u)\\b\\w\\w+\\b";
                Pattern pattern = Pattern.compile(regex);
                Matcher matcher = pattern.matcher(lowerData);
                List<String> result = new ArrayList<>();

                while (matcher.find()) {
                    if(!matcher.group().isBlank() && !this.stopWordList.contains(matcher.group()) && this.adjWord.contains(matcher.group())){
                        result.add(matcher.group());
                    }
                }
                GenericData.Record  avroRecord = new GenericData.Record(schema);
                avroRecord.put("id","0");
                avroRecord.put("reviewerID",jsonNode.get("reviewerID").asText());
                avroRecord.put("asin",jsonNode.get("asin").asText());
                avroRecord.put("reviewerName",jsonNode.get("reviewerName").asText());
                avroRecord.put("adjectiveWord",result);
                avroRecord.put("reviewText",jsonNode.get("reviewText").asText());
                context.write(new AvroKey<>(avroRecord), new AvroValue<>(avroRecord));
            }
        }
    }
}
