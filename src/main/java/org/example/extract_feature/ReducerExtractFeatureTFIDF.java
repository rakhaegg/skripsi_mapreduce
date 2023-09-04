package org.example.extract_feature;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroMultipleOutputs;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class ReducerExtractFeatureTFIDF extends Reducer<AvroKey<Integer>, AvroValue<GenericRecord>,AvroKey<GenericRecord>,NullWritable> {
    private Logger logger = LoggerFactory.getLogger(ReducerExtractFeatureTFIDF.class);
    private long lengthDocument = 0;
    private Map<Long,Integer> map = new HashMap<>();
    private AvroMultipleOutputs multipleOutputs;
    private static final Schema schema4 = new Schema.Parser().parse(
            "{" +
                    "  \"type\": \"record\"," +
                    "  \"name\": \"finalFeature\"," +
                    "  \"fields\": [" +
                    "    {\"name\" : \"id\",\"type\": \"string\"}," +
                    "    {\"name\" :  \"feature\", \"type\": {" +
                    "      \"type\" :\"map\"," +
                    "      \"values\": \"double\"" +
                    "    }" +
                    "    }" +
                    "  ]" +
                    "}"
    );
    private Map<Long,Long> listContainIDInitCentroid = new HashMap<>();
    @Override
    protected void setup(Reducer<AvroKey<Integer>, AvroValue<GenericRecord>, AvroKey<GenericRecord>, NullWritable>.Context context) {
        TypeToken<Map<Long,Long>> typeToken = new TypeToken<Map<Long,Long>>(){};
        Gson gson = new Gson();

        this.listContainIDInitCentroid =gson.fromJson(context.getConfiguration().get("centroid"),typeToken.getType());
        multipleOutputs = new AvroMultipleOutputs(context);
    }

    @Override
    protected void reduce(AvroKey<Integer> key, Iterable<AvroValue<GenericRecord>> values, Reducer<AvroKey<Integer>, AvroValue<GenericRecord>, AvroKey<GenericRecord>, NullWritable>.Context context) throws IOException, InterruptedException {
        for(AvroValue<GenericRecord> value: values){
            int length = Integer.parseInt(value.datum().get("length").toString());
            Map<String,Map<String,Double>> data = (Map<String, Map<String, Double>>) value.datum().get("feature");
            AtomicReference<Double> denominator = new AtomicReference<>((double) 0);
            Map<String,Double> temp = data.entrySet().stream().map((e)->{
               String word = e.getKey();
               double idf = e.getValue().get("idf");
               double tf = e.getValue().get("tf");
               double finalTF;
               if(length == 0)
                   finalTF = 0;
               else
                   finalTF = tf/length;
               denominator.set(denominator.get() + Math.pow((finalTF*idf),2));
               return Map.entry(word,(finalTF * idf));
           }).collect(Collectors.toMap(Map.Entry::getKey,Map.Entry::getValue));

            Double denominatorValue = Math.sqrt(denominator.get());
            Map<String,Double> finalResult = temp.entrySet().stream().map(itemTemp->{
                Double result = itemTemp.getValue()/denominatorValue;
                if(result.isNaN())
                    result = 0.0;
                BigDecimal decimal = new BigDecimal(result);
                BigDecimal round = decimal.setScale(6, RoundingMode.HALF_UP);
                return Map.entry(itemTemp.getKey(),round.doubleValue());
            }).collect(Collectors.toMap(Map.Entry::getKey,Map.Entry::getValue));

            GenericRecord newRecord = new GenericData.Record(schema4);
            newRecord.put("feature",finalResult);
            long id = Long.parseLong(value.datum().get("id").toString());
            if(this.listContainIDInitCentroid.containsKey(id)){
                newRecord.put("id",String.valueOf(this.listContainIDInitCentroid.get(id)));
                multipleOutputs.write("text",new AvroKey<>(newRecord),NullWritable.get(),"centroid");
            }
            newRecord.put("id",String.valueOf(id));
            context.write(new AvroKey<>(newRecord),NullWritable.get());
        }
    }
    /*
    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Reducer<IntWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        long totalDocument = lengthDocument;

        for(Text value: values){
            String[] data = value.toString().split("\t");
            int length = Integer.parseInt(data[2]);

            AtomicReference<Double> denominator = new AtomicReference<>((double) 0);
            List<String> as = Arrays.stream(data[1].substring(1, data[1].length()-1).split(", "))
                    .map(item -> {
                        String[] xx = item.substring(1 , item.length()-1).split(",");
                        String word = xx[0];
                        double df = Integer.parseInt(xx[1]);
                        double tf = Integer.parseInt(xx[2]);
                        double idf = Math.log((totalDocument+1)/(df+1)) + 1;
                        double finalTF;
                        if(length==0)
                            finalTF =0;
                        else
                            finalTF =tf/length;
                        return "("+word+":"+(idf*finalTF)+")";
                    })
                    .peek(item -> {
                        double result = Math.pow(
                                Double.parseDouble(item.substring(1, item.length()-1).split(":")[1]),
                                2);
                        denominator.set(denominator.get() + result);
                    }).collect(Collectors.toList());
            denominator.set(Math.sqrt(denominator.get()));
            List<String> qw = as.stream().map(item->{
                String[] x = item.substring(1 , item.length()-1).split(":");
                String word = x[0];
                Double tf_idf = Double.parseDouble(x[1]);
                Double result = tf_idf/denominator.get();
                if(result.isNaN())
                    result = 0.0;
                BigDecimal decimal = new BigDecimal(result);
                BigDecimal round = decimal.setScale(2, RoundingMode.HALF_UP);
                return "("+word+":"+round.doubleValue()+")";
            }).collect(Collectors.toList());

            if(map.containsKey(Long.parseLong(data[0]))){
                multipleOutputs.write("text", map.get(Long.parseLong(data[0])),
                        qw.toString(), "centroid");
                multipleOutputs.write("text", data[0], qw.toString());
            }else{
                multipleOutputs.write("text", data[0], qw.toString());
            }

        }
    }
    */

    @Override
    protected void cleanup(Reducer<AvroKey<Integer>, AvroValue<GenericRecord>, AvroKey<GenericRecord>, NullWritable>.Context context) throws IOException, InterruptedException {
        multipleOutputs.close();
    }
}
