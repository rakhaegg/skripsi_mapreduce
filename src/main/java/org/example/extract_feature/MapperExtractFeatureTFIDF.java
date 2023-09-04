package org.example.extract_feature;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class MapperExtractFeatureTFIDF extends Mapper<AvroKey<GenericRecord>, NullWritable,AvroKey<Integer>, AvroValue<GenericRecord>> {
    private static Logger logger = LoggerFactory.getLogger(MapperExtractFeatureTFIDF.class);
    private Map<String, Integer> map = new HashMap<>();
    private Map<String, Integer> fixFeature = new HashMap<>();
    private static final Schema schema3 = new Schema.Parser().parse(
            "{" +
                    "  \"type\": \"record\"," +
                    "  \"name\": \"InputFeature\"," +
                    "  \"fields\": [" +
                    "    {\"name\": \"id\", \"type\" : \"string\"}," +
                    "    {\"name\": \"length\", \"type\": \"int\"}," +
                    "    {\"name\": \"feature\", \"type\": {" +
                    "      \"type\": \"map\"," +
                    "      \"values\": {" +
                    "        \"type\": \"map\"," +
                    "        \"values\": \"double\"" +
                    "      }" +
                    "    }}" +
                    "  ]" +
                    "}"
    );

    private Map<String,Double> tempDocumentFrequency = new HashMap<>();
    @Override
    protected void setup(Mapper<AvroKey<GenericRecord>, NullWritable, AvroKey<Integer>, AvroValue<GenericRecord>>.Context context) throws IOException, InterruptedException {
        URI[] uris  = context.getCacheFiles();
        logger.info(Arrays.toString(uris));
        FileSystem fs = FileSystem.get(context.getConfiguration());
        Path pathDocumentFrequency = new Path(uris[0]);
        Path pathadjectiveWord = new Path(uris[1]);
        Path pathContainLengthDocument = new Path(uris[2]);
        AtomicLong lengthDocument = new AtomicLong();
        try(BufferedInputStream inputStream = new BufferedInputStream(fs.open(pathDocumentFrequency))){
            DataFileStream<GenericRecord> dataFileStream = new DataFileStream<>(inputStream, new GenericDatumReader<>());
            Schema schema = dataFileStream.getSchema();
            GenericRecord temp = null;
            while (dataFileStream.hasNext()) {
                temp = dataFileStream.next();
                tempDocumentFrequency.put(temp.get("word").toString(),Double.parseDouble(temp.get("idf").toString()));
            }
        }
        try(BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fs.open(pathContainLengthDocument)))){
            String line;
            while ((line = bufferedReader.readLine()) != null){
                lengthDocument.set(Long.parseLong(line));
            }
        }
        Set<String> listadjWord = new HashSet<>();
        try(BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fs.open(pathadjectiveWord)))){
            String line;
            while ((line = bufferedReader.readLine()) != null){
                listadjWord.add(line);
            }
        }
        listadjWord.forEach(item->{
            if(!tempDocumentFrequency.containsKey(item)){
                tempDocumentFrequency.put(item,Math.log(((double) lengthDocument.get()+1)/(0+1)) +1);
            }
        });
    }

    @Override
    protected void map(AvroKey<GenericRecord> key, NullWritable value, Mapper<AvroKey<GenericRecord>, NullWritable, AvroKey<Integer>, AvroValue<GenericRecord>>.Context context) throws IOException, InterruptedException {
        GenericRecord record = key.datum();
        List<String> adjectiveWord = (List<String>) record.get("adjectiveWord");

        Map<String,Long> hashMapAdjectiveWord = adjectiveWord
                .stream().collect(Collectors.groupingBy(e->e,Collectors.counting()));
        HashMap<String,Map<String,Double>> stringMapMap = new HashMap<>();
        tempDocumentFrequency.forEach((word,idf)->{
            HashMap<String,Double> valuex = new HashMap<>();
            valuex.put("idf", idf);
            valuex.put("tf", (double) hashMapAdjectiveWord.getOrDefault(word,0L));
            stringMapMap.put(word,valuex);
        });
        GenericData.Record avroRecord = new GenericData.Record(schema3);
        avroRecord.put("id",record.get("id").toString());
        avroRecord.put("feature",stringMapMap);
        avroRecord.put("length",adjectiveWord.size());
        context.write(new AvroKey<>(adjectiveWord.size()),new AvroValue<>(avroRecord));
    }
}
