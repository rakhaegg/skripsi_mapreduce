package org.example.extract_feature;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapreduce.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.StreamSupport;

public class JobExtractFeature {
    private Configuration configuration;
    private Path inputFolder;
    private Path outputFolder;
    private FileSystem fileSystem;
    private static Logger logger = LoggerFactory.getLogger(JobExtractFeature.class);
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
    public JobExtractFeature(String inputFolder,String outputFolder,
                             Configuration configuration) throws IOException {
        this.inputFolder = new Path(inputFolder);
        this.outputFolder = new Path(outputFolder);
        this.configuration = configuration;
        this.fileSystem = FileSystem.get(this.configuration);
    }
    public void runJobFirst() throws IOException, InterruptedException, ClassNotFoundException {
        Job job = Job.getInstance(this.configuration,"Job Document Frequency");
        job.setJarByClass(JobExtractFeature.class);
        job.setMapperClass(MapperExtractFeatureDocument.class);
        job.setReducerClass(ReducerExtractFeatureDocument.class);

        if(this.fileSystem.exists(outputFolder)){
            this.fileSystem.delete(outputFolder,true);
        }
        AvroKeyInputFormat.addInputPath(job,inputFolder);
        FileOutputFormat.setOutputPath(job,outputFolder);
        job.setInputFormatClass(AvroKeyInputFormat.class); // Menentukan Input Format Avro
        job.setOutputFormatClass(AvroKeyOutputFormat.class);
        AvroJob.setInputKeySchema(job,schema); // Menentukan skema avro untuk input
        AvroJob.setMapOutputKeySchema(job,Schema.create(Schema.Type.STRING));
        AvroJob.setMapOutputValueSchema(job,Schema.create(Schema.Type.LONG));
        AvroJob.setOutputKeySchema(job,schema2);
        try {
            job.addCacheFile(new URI("hdfs://master:9000/output/"+this.configuration.get("name")+"/jumlah_dokumen.txt"));
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
        boolean success = job.waitForCompletion(true);


        if(success){
            long lengthDocument = 0;
            Path pathContainLengthDocument = new Path("/output/"+this.configuration.get("name")+"/jumlah_dokumen.txt");
            try(BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(this.fileSystem.open(pathContainLengthDocument)))){
                String line;
                while ((line = bufferedReader.readLine()) != null){
                    lengthDocument = Long.parseLong(line);
                }
            }
            long minRandom = 0;
            int k = Integer.parseInt(this.configuration.get("k"));
            long maxRandom = lengthDocument;
            Map<Long,Long> listContainIDInitCentroid = new HashMap<>();

            Random random = new Random();
            LongStream randomNumbers=
                    random.longs(minRandom,maxRandom +1).distinct().limit(k);
            AtomicLong count = new AtomicLong(0L);
            randomNumbers.forEach(item ->{
                listContainIDInitCentroid.put(item,count.get());
                count.set(count.get() + 1);
            });
            Gson gsonObj = new Gson();
            logger.info("INIT CENTROID : " + listContainIDInitCentroid);
            configuration.set("centroid",gsonObj.toJson(listContainIDInitCentroid));
            Job jobExtractFeature = Job.getInstance(this.configuration,"Job TFxIDF");

            jobExtractFeature.setJarByClass(JobExtractFeature.class);
            jobExtractFeature.setMapperClass(MapperExtractFeatureTFIDF.class);
            jobExtractFeature.setReducerClass(ReducerExtractFeatureTFIDF.class);
            Path outputFolder =
                    new Path("/output_2/"+configuration.get("name"));
            Path inputFolder
                    = new Path("/output/"+configuration.get("name")+"/part-r-00000.avro");
            if(this.fileSystem.exists(outputFolder)){
                this.fileSystem.delete(outputFolder,true);
            }
            try {
                jobExtractFeature.addCacheFile(new URI("hdfs://master:9000/output_1/"+this.configuration.get("name")+"/part-r-00000.avro"));
                jobExtractFeature.addCacheFile(new URI("hdfs://master:9000/adj.txt"));
                jobExtractFeature.addCacheFile(new URI("hdfs://master:9000/output/"+this.configuration.get("name")+"/jumlah_dokumen.txt"));
            } catch (URISyntaxException e) {
                throw new RuntimeException(e);
            }
            jobExtractFeature.setPartitionerClass(HashPartitioner.class);
           // jobExtractFeature.setNumReduceTasks(4);
            jobExtractFeature.setInputFormatClass(AvroKeyInputFormat.class);
            AvroKeyInputFormat.addInputPath(jobExtractFeature,inputFolder);
            AvroJob.setInputKeySchema(jobExtractFeature,schema);
            AvroJob.setMapOutputValueSchema(jobExtractFeature,schema3);
            AvroJob.setMapOutputKeySchema(jobExtractFeature,Schema.create(Schema.Type.INT));
            AvroJob.setOutputKeySchema(jobExtractFeature,schema4);
            AvroMultipleOutputs.addNamedOutput(jobExtractFeature,"text", AvroKeyOutputFormat.class,schema4);
            AvroKeyOutputFormat.setCompressOutput(jobExtractFeature,true);
            AvroKeyOutputFormat.setOutputCompressorClass(jobExtractFeature, SnappyCodec.class);
            AvroKeyOutputFormat.setOutputPath(jobExtractFeature,outputFolder);
            jobExtractFeature.setOutputFormatClass(AvroKeyOutputFormat.class);
             boolean success2 =  jobExtractFeature.waitForCompletion(true);
             if(success2){
                 String ip = "192.168.88.2";
                 String port = "14000";
                 String outputFolderCentroid = "/output_2/"+this.configuration.get("name");
                 String urlPath = "http://"+ip+":"+port+"/webhdfs/v1"+outputFolderCentroid
                         +"?op=LISTSTATUS&" +
                         "user.name=hadoopuser";
                 AtomicReference<String> outputStringCentroid = new AtomicReference<>();
                 try(CloseableHttpClient closeableHttpClient = HttpClientBuilder.create().build()){
                     HttpGet httpGet = new HttpGet(urlPath);
                     closeableHttpClient.execute(httpGet,response->{
                         HttpEntity http = response.getEntity();
                         outputStringCentroid.set(EntityUtils.toString(http));
                         EntityUtils.consume(http);
                         return response;
                     });
                     ObjectMapper objectMapper = new ObjectMapper();
                     List<String> listCentroidAsJsonNode = StreamSupport.stream(
                                     Spliterators.spliteratorUnknownSize(objectMapper.readTree(outputStringCentroid.get()).get("FileStatuses").get("FileStatus")
                                             .iterator(), Spliterator.ORDERED),false)
                             .filter((item)->item.get("pathSuffix").asText().split("-")[0].equals("centroid"))
                             .map(item-> "/output_2/"+this.configuration.get("name")+"/"+item.get("pathSuffix").asText())
                             .collect(Collectors.toList());

                    Map<String,Map<String,String>> tempDocumentFrequency = new HashMap<>();
                    List<GenericRecord> genericRecordList = new ArrayList<>();
                     listCentroidAsJsonNode.forEach(item->{
                         Path path = new Path(item);
                         try(BufferedInputStream inputStream = new BufferedInputStream(fileSystem.open(path))){
                             DataFileStream<GenericRecord> dataFileStream = new DataFileStream<>(inputStream, new GenericDatumReader<>());
                             GenericRecord temp = null;
                             while((dataFileStream.hasNext())){
                                 temp = dataFileStream.next();;
                                 genericRecordList.add(temp);
                             }
                         } catch (IOException e) {
                             throw new RuntimeException(e);
                         }
                     });
                     writeAvroFileToHDFS("/output_2/"+this.configuration.get("name") +"/" + "centroid.avro",genericRecordList);

                 }
             }
        }
    }
    private void writeAvroFileToHDFS(String path, List<GenericRecord> list) throws IOException {
        GenericDatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema4);
        try(DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter)){
            dataFileWriter.setCodec(CodecFactory.snappyCodec());
            Path outputFileNewCentroid = new Path(  path);
            dataFileWriter.create(schema4,this.fileSystem.create(outputFileNewCentroid));
            for(GenericRecord record : list){
                dataFileWriter.append(record);
            }
        }
    }
}
