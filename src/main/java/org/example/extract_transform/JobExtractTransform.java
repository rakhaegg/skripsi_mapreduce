package org.example.extract_transform;

import org.apache.avro.Schema;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class JobExtractTransform {
    private Configuration configuration;
    private Path inputFolder;
    private Path outputFolder;
    private FileSystem fileSystem;
    private static Logger logger = LoggerFactory.getLogger(JobExtractTransform.class);
    private long counterDocument = 0;
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
    public JobExtractTransform(String inputFolder, String outputFolder,
                               Configuration configuration) throws IOException {
        this.inputFolder =new Path(inputFolder);
        this.outputFolder = new Path(outputFolder);
        this.configuration = configuration;
        this.fileSystem = FileSystem.get(configuration);
    }
    public Job runJob() throws IOException{
        Job job = Job.getInstance(this.configuration , "Extract Transform");
        job.setJarByClass(JobExtractTransform.class);
        job.setMapperClass(MapperExtractTransform.class);
        job.setReducerClass(ReducerExtractTransform.class);

        if(this.fileSystem.exists(outputFolder)){
            this.fileSystem.delete(outputFolder,true);
        }
        try {
            job.addCacheFile(new URI("hdfs://master:9000/adj.txt"));
            job.addCacheFile(new URI("hdfs://master:9000/stopwords_en.txt"));

        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
        FileInputFormat.addInputPath(job,inputFolder);
        FileOutputFormat.setOutputPath(job,outputFolder);
        FileOutputFormat.setCompressOutput(job,true);
        FileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(AvroKeyOutputFormat.class);

        AvroJob.setMapOutputKeySchema(job,schema);
        AvroJob.setMapOutputValueSchema(job,schema);

        AvroJob.setOutputKeySchema(job,schema);

        return job;
    }



}
