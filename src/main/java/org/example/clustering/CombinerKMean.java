package org.example.clustering;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.StreamSupport;

public class CombinerKMean extends Reducer<AvroKey<Integer>, AvroValue<GenericRecord> , AvroKey<Integer>,AvroValue<GenericRecord>> {
    private Logger logger = LoggerFactory.getLogger(CombinerKMean.class);

    @Override
    protected void reduce(AvroKey<Integer> key, Iterable<AvroValue<GenericRecord>> values, Reducer<AvroKey<Integer>, AvroValue<GenericRecord>, AvroKey<Integer>, AvroValue<GenericRecord>>.Context context) throws IOException, InterruptedException {
        HashMap<String,Double> sumMap = new HashMap<>();
        List<Integer> integerList = new ArrayList<>();
        final Schema[] schema = {null};
        AtomicReference<Double> sse = new AtomicReference<>(0.0);
        StreamSupport.stream(values.spliterator(),false).forEach((itemValues)->{
            if(schema[0] == null)
                schema[0] = itemValues.datum().getSchema();
            Map<String,Double> temp = (Map<String, Double>) itemValues.datum().get("agg_feature");
            temp.entrySet().stream().map(item-> Map.entry(item.getKey(),item.getValue()))
                    .forEach(item-> sumMap.merge(item.getKey(),item.getValue(),Double::sum));
            GenericData.Array<Integer>  aaa = (GenericData.Array<Integer>) itemValues.datum().get("list_id");
            integerList.add(aaa.get(0));
            sse.set(sse.get() + Math.pow( (double) itemValues.datum().get("sse"),2));
        });
        logger.info(String.valueOf(integerList.size()));
        logger.info(integerList.toString());
        GenericData.Record dataRecord = new GenericData.Record(schema[0]);
        dataRecord.put("agg_feature",sumMap);
        dataRecord.put("cluster",key.datum());
        dataRecord.put("list_id",integerList);
        dataRecord.put("sse",sse.get());
        context.write(key,new AvroValue<>(dataRecord));
    }
}

