package org.example.clustering;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class MapperKMean extends Mapper<AvroKey<GenericRecord>, NullWritable, AvroKey<Integer>,AvroValue<GenericRecord>> {
    private Logger logger = LoggerFactory.getLogger(MapperKMean.class);
    private Map<Integer, Map<String,Double>> centroid = new HashMap<>();
    private static final Schema schema5 = new Schema.Parser().parse(
            "{" +
                    "  \"type\": \"record\"," +
                    "  \"name\": \"intermediate\"," +
                    "  \"fields\": [" +
                    "    {\"name\": \"cluster\" , \"type\" :  \"int\" }," +
                    "    {\"name\" : \"list_id\" , \"type\":{" +
                    "      \"type\": \"array\"," +
                    "      \"items\": \"int\"" +
                    "      }," +
                    "      \"default\": []" +
                    "    }," +
                    "    {" +
                    "      \"name\": \"agg_feature\",\"type\": {" +
                    "        \"type\": \"map\"," +
                    "        \"values\": \"double\"" +
                    "    }" +
                    "    }," +
                    "    {" +
                    "      \"name\": \"sse\", \"type\": \"double\"" +
                    "    }" +
                    "  ]" +
                    "}"
    );
    @Override
    protected void setup(Mapper<AvroKey<GenericRecord>,NullWritable, AvroKey<Integer>, AvroValue<GenericRecord>>.Context context) throws IOException, InterruptedException {
        logger.info("SETUP");
        logger.info(Arrays.toString(context.getCacheFiles()));
        FileSystem fileSystem = FileSystem.get(context.getConfiguration());
        URI[] urlS = context.getCacheFiles();
        try(BufferedInputStream inputStream = new BufferedInputStream(fileSystem.open(new Path(urlS[0])))){
            DataFileStream<GenericRecord> dataFileStream = new DataFileStream<>(inputStream,new GenericDatumReader<>());
            Schema schema = dataFileStream.getSchema();
            GenericRecord temp = null;
            while (dataFileStream.hasNext()){
                temp = dataFileStream.next();
                Integer indexCluster = Integer.parseInt(temp.get("id").toString());
                Map<Utf8, Double> centroid = (Map<Utf8, Double>) temp.get("feature");
                this.centroid.put(indexCluster,centroid.
                        entrySet()
                        .stream()
                        .map(item->Map.entry(item.getKey().toString(),Double.parseDouble(item.getValue().toString())))
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
            }

        }
        centroid.forEach((key,value)->{
            logger.info(String.valueOf(key));
            logger.info(value.toString());
        });
    }

    @Override
    protected void map(AvroKey<GenericRecord> key, NullWritable value, Mapper<AvroKey<GenericRecord>, NullWritable, AvroKey<Integer>, AvroValue<GenericRecord>>.Context context) throws IOException, InterruptedException {
        Map<String,Double> mapInput = (Map<String, Double>) key.datum().get("feature");
        Map<Integer,Double> distanceBasedCluster = new HashMap<>();
        centroid.forEach((keyCentroid,valueCentroid)->{
            AtomicReference<Double> sum = new AtomicReference<>((double) 0);
            valueCentroid.forEach((word,wordValue)->{
                Double inputTFIDF = mapInput.get(word);
                Double result = Math.pow((inputTFIDF-wordValue),2);
                sum.set(sum.get() + result);
            });
            distanceBasedCluster.put(keyCentroid,Math.sqrt(sum.get()));
        });
        Map.Entry<Integer,Double> minEntry = distanceBasedCluster.entrySet().stream()
                .min(Map.Entry.comparingByValue())
                .orElse(null);

        assert minEntry != null;
        GenericData.Record avroRecord = new GenericData.Record(schema5);
        avroRecord.put("agg_feature",mapInput);
        Integer[] temp = new Integer[1];
        temp[0] = Integer.parseInt(key.datum().get("id").toString());
        avroRecord.put("cluster",minEntry.getValue());
        avroRecord.put("list_id", temp);
        avroRecord.put("sse",minEntry.getValue());
        context.write(new AvroKey<>(minEntry.getKey()),new AvroValue<>(avroRecord));
    }
    /*
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, IntWritable, Text>.Context context) throws IOException, InterruptedException {
        String[] splitInputValue = value.toString().split("\t");
        Map<String,Double> mapInputValue = new HashMap<>();
        Map<Integer,Double> distanceBasedCluster = new HashMap<>();
        Arrays.stream(splitInputValue[1]
                .substring(1, splitInputValue[1].length() - 1).split(", "))
                        .forEach(item->{
                            String[] itemSplit = item.substring(1, item.length()-1).split(":");
                            mapInputValue.put(itemSplit[0],Double.parseDouble(itemSplit[1]));
                        });
        centroid.forEach((keyCentroid,valueCentroid)->{
            AtomicReference<Double> sum = new AtomicReference<>((double) 0);
            valueCentroid.forEach((word,wordValue)->{
                Double inputTFIDF = mapInputValue.get(word);
                Double result = Math.pow((inputTFIDF-wordValue),2);
                sum.set(sum.get() + result);
            });
            distanceBasedCluster.put(keyCentroid,Math.sqrt(sum.get()));
        });

        Map.Entry<Integer,Double> maxEntry = distanceBasedCluster.entrySet().stream()
                        .min(Map.Entry.comparingByValue())
                                .orElse(null);
        assert maxEntry != null;
        IntWritable intWritable = new IntWritable();
        intWritable.set(maxEntry.getKey());
        context.write(intWritable,new Text(value + "\t" + maxEntry.getValue()));
    }

     */
}
