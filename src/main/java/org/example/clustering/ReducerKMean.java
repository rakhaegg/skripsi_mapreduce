package org.example.clustering;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.StreamSupport;

public class ReducerKMean extends Reducer<AvroKey<Integer>, AvroValue<GenericRecord>,AvroKey<GenericRecord>, NullWritable> {
    private static Logger logger = LoggerFactory.getLogger(ReducerKMean.class);
    private MultipleOutputs<IntWritable,Text> multipleOutputs;

    @Override
    protected void reduce(AvroKey<Integer> key, Iterable<AvroValue<GenericRecord>> values, Reducer<AvroKey<Integer>, AvroValue<GenericRecord>,AvroKey<GenericRecord>,NullWritable>.Context context) throws IOException, InterruptedException {
        logger.info("INDEX CLUSTER : " + key.datum().toString());
        HashMap<String,Double> sumMap = new HashMap<>();
        List<Integer> listID = new ArrayList<>();
        final Schema[] schema = {null};
        AtomicReference<Double> sse = new AtomicReference<>(0.0);
        StreamSupport.stream(values.spliterator(),false).forEach((itemValues)->{
            if(schema[0] == null)
                schema[0] = itemValues.datum().getSchema();
            Map<String,Double> temp = (Map<String, Double>) itemValues.datum().get("agg_feature");
            temp.entrySet().stream().map(item-> Map.entry(item.getKey(),item.getValue()))
                    .forEach(item-> sumMap.merge(item.getKey(),item.getValue(),Double::sum));
            GenericData.Array<Integer>  aaa = (GenericData.Array<Integer>) itemValues.datum().get("list_id");
            listID.add(aaa.get(0));
            sse.set(sse.get() + Math.pow( (double) itemValues.datum().get("sse"),2));
        });

        logger.info(String.valueOf(listID.size()));
        sumMap.forEach((keySumMap,valueSumMap) -> {
            BigDecimal decimal = new BigDecimal(valueSumMap/listID.size());
            BigDecimal round = decimal.setScale(10, RoundingMode.HALF_UP);
            sumMap.put(keySumMap,round.doubleValue());
        });
        GenericData.Record dataRecord = new GenericData.Record(schema[0]);
        dataRecord.put("agg_feature",sumMap) ;
        dataRecord.put("cluster",key.datum());
        dataRecord.put("list_id",listID);
        dataRecord.put("sse",sse.get());
        context.write(new AvroKey<>(dataRecord),NullWritable.get());
    }
/*
    @Override
    protected void setup(Reducer<IntWritable, Text, IntWritable, Text>.Context context) throws IOException, InterruptedException {
        multipleOutputs = new MultipleOutputs<>(context);
    }

    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Reducer<IntWritable, Text, IntWritable, Text>.Context context) throws IOException, InterruptedException {
        Map<String, Double> mapAverage = new HashMap<>();
        AtomicInteger count = new AtomicInteger();
        AtomicReference<Double> sse = new AtomicReference<>(0.0);
        StreamSupport.stream(values.spliterator(), false).forEach(value->{
            String[] splitValue = value.toString().split("\t");
            String documentID = splitValue[0];
            BigDecimal decimal = BigDecimal.valueOf(Double.parseDouble(splitValue[2]));

            BigDecimal round = decimal.setScale(10, RoundingMode.HALF_UP);
            sse.set(sse.get() + Math.pow(round.doubleValue(), 2));
            Arrays.stream(splitValue[1].substring(1, splitValue[1].length() - 1).split(", "))
                    .forEach(item->{
                        String[] splitItem  = item.substring(1, item.length()-1).split(":");
                        String word = splitItem[0];
                        Double valueWord = Double.parseDouble(splitItem[1]);
                        if(!mapAverage.containsKey(word)){
                            mapAverage.put(word,valueWord);
                        }else{
                            Double oldValue = mapAverage.get(word);
                            mapAverage.put(word, (oldValue+valueWord));
                        }
                    });
            count.set(count.get() + 1);
            try {
                multipleOutputs.write("text",documentID ,
                        round.doubleValue(), key.toString());
            } catch (IOException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
        multipleOutputs.write("text","total : ", sse.get(), "sse-"+key);
        List<String> dataResult = new ArrayList<>();
        mapAverage.forEach((keyValue,value)->{
            BigDecimal decimal = new BigDecimal(value/count.get());
            BigDecimal round = decimal.setScale(10, RoundingMode.HALF_UP);
            dataResult.add("("+keyValue+":"+round.doubleValue()+")");
        });
        context.write(key,new Text(dataResult.toString()));
    }

    @Override
    protected void cleanup(Reducer<IntWritable, Text, IntWritable, Text>.Context context) throws IOException, InterruptedException {
        multipleOutputs.close();
    }

     */
}
