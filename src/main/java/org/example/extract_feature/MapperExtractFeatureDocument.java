package org.example.extract_feature;

import org.apache.avro.generic.GenericData;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class MapperExtractFeatureDocument extends Mapper<AvroKey<GenericData.Record>, NullWritable,AvroKey<String>, AvroValue<Long>> {
    private static Logger logger = LoggerFactory.getLogger(MapperExtractFeatureDocument.class);

    @Override
    protected void map(AvroKey<GenericData.Record> key, NullWritable value,
                       Mapper< AvroKey<GenericData.Record>, NullWritable, AvroKey<String>, AvroValue<Long>>.Context context) throws IOException, InterruptedException {
        GenericData.Record avroRecord = key.datum();
        List<String> asd = (List<String>) avroRecord.get("adjectiveWord");
        Set<String> uniqueString = new HashSet<>(asd);
        uniqueString.forEach(item ->{
            try {
                context.write(new AvroKey<>(item),new AvroValue<>(1L));
            } catch (IOException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
    }


}
