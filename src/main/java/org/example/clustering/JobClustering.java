package org.example.clustering;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JobClustering {
    private Configuration configuration;
    private Path inputFolder;
    private Path outputFolder;
    private static Logger logger = LoggerFactory.getLogger(JobClustering.class);
    private int iteration;
    private boolean statusConvergen = false;
    private Map<String,Double> sse = new HashMap<>();

    public Map<String, Double> getSse() {
        return sse;
    }

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
    public boolean isStatusConvergen() {
        return statusConvergen;
    }

    public JobClustering(
            String inputFolder,
            String outputFolder,
            Configuration configuration,
            int iteration
    ) throws IOException {
        this.inputFolder = new Path(inputFolder);
        this.outputFolder = new Path(outputFolder);
        this.configuration = configuration;
        this.iteration = iteration;
    }

    public void runJobClustering() throws IOException, InterruptedException, ClassNotFoundException {
        Job job = Job.getInstance(this.configuration,"Job Clustering");
        job.setJarByClass(JobClustering.class);
        job.setMapperClass(MapperKMean.class);
        job.setReducerClass(ReducerKMean.class);

        FileSystem fs = FileSystem.get(this.configuration);
        if(fs.exists(outputFolder)){
            fs.delete(outputFolder,true);
        }
        try {
            job.addCacheFile(new URI("hdfs://master:9000/output_2/"
                    +this.configuration.get("name")+"/centroid-update-r-00000.avro"));
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
        job.setInputFormatClass(AvroKeyInputFormat.class);
        FileStatus[] fileStatuses = fs.listStatus(new Path("/output_2/"+this.configuration.get("name")));
        List<Path> stringList = new ArrayList<>();
        for(FileStatus fileStatus : fileStatuses){
            if(fileStatus.isFile()){
                if(fileStatus.getPath().getName().split("-")[0].equals("part")){
                    stringList.add(new Path("/output_2/"+this.configuration.get("name")+"/"+fileStatus.getPath().getName()));
                }
            }
        }

        if(this.iteration == 1){
            List<GenericRecord> genericRecordList = new ArrayList<>();
            Schema schema = null;
            try(BufferedInputStream inputStream = new BufferedInputStream(fs.open(new Path("/output_2/"+this.configuration.get("name") +"/" + "centroid.avro")))){
                DataFileStream<GenericRecord> dataFileStream = new DataFileStream<>(inputStream,new GenericDatumReader<>());
                GenericRecord temp = null;
                while(dataFileStream.hasNext()){
                    temp = dataFileStream.next();
                    if(schema == null)
                        schema = temp.getSchema();
                    genericRecordList.add(temp);
                }
            }
            writeAvroFileToHDFS("/output_2/"+this.configuration.get("name") +"/" + "centroid-update-r-00000.avro",
                    genericRecordList,schema,fs);
        }
        for(Path path : stringList){
            MultipleInputs.addInputPath(job,path,AvroKeyInputFormat.class, MapperKMean.class);
        }

        AvroKeyInputFormat.setMaxInputSplitSize(job, 10485760                      );
        AvroKeyInputFormat.setMinInputSplitSize(job,10485760          );
        AvroJob.setInputKeySchema(job,schema4);
        AvroJob.setMapOutputKeySchema(job,Schema.create(Schema.Type.INT));
        AvroJob.setMapOutputValueSchema(job,schema5);
        AvroJob.setOutputValueSchema(job,Schema.create(Schema.Type.NULL));
        AvroJob.setOutputKeySchema(job, schema5);

        AvroKeyOutputFormat.setOutputPath(job,outputFolder);
        AvroKeyOutputFormat.setCompressOutput(job,true);
        AvroKeyOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);
        job.setOutputFormatClass(AvroKeyOutputFormat.class);
        job.setNumReduceTasks(2);
        boolean success  =job.waitForCompletion(true);
        if(success) {
            FileStatus[] fileOutput3 = fs.listStatus(new Path("/output_3/" + this.configuration.get("name")));
            List<Path> listPathNewCentroid = new ArrayList<>();
            for (FileStatus fileItemOutput3 : fileOutput3) {
                if (fileItemOutput3.isFile()) {
                    if (fileItemOutput3.getPath().getName().split("-")[0].equals("part")) {
                        listPathNewCentroid.add(new Path("/output_3/" + this.configuration.get("name") + "/" + fileItemOutput3.getPath().getName()));
                    }

                }
            }
            Map<Integer,Map<String,Double>> newCentroid = new HashMap<>();
            // ! GET NEW CENTROID
            List<GenericRecord> genericRecordList = new ArrayList<>();
            listPathNewCentroid.forEach(itemPath ->{
                try(BufferedInputStream inputStream = new BufferedInputStream(fs.open(itemPath))){
                    DataFileStream<GenericRecord> dataFileStream = new DataFileStream<>(inputStream, new GenericDatumReader<>());
                    GenericRecord temp = null;
                    while(dataFileStream.hasNext()){
                        temp = dataFileStream.next();
                        Integer cluster = Integer.parseInt(temp.get("cluster").toString());
                        newCentroid.put(cluster,(Map<String, Double>) temp.get("agg_feature"));
                        GenericRecord genericRecord = new GenericData.Record(schema4);
                        genericRecord.put("id",cluster.toString());
                        genericRecord.put("feature", temp.get("agg_feature"));
                        sse.put(temp.get("cluster").toString(),(double)temp.get("sse"));
                        genericRecordList.add(genericRecord);
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
            // GET OLD CENTROID
            Path pathOldCentroid = new Path("/output_2/"+this.configuration.get("name")+"/centroid-update-r-00000.avro");
            Schema schemaOld = null;
            Map<Integer,Map<String,Double>> oldCentroid = new HashMap<>();
            try(BufferedInputStream inputStream = new BufferedInputStream(fs.open(pathOldCentroid))){
                DataFileStream<GenericRecord> dataFileStream = new DataFileStream<>(inputStream, new GenericDatumReader<>());
                GenericRecord temp = null;
                while(dataFileStream.hasNext()){
                    temp = dataFileStream.next();
                    if(schemaOld == null)
                        schemaOld = temp.getSchema();
                    Integer cluster = Integer.parseInt(temp.get("id").toString());
                    oldCentroid.put(cluster,(Map<String, Double>) temp.get("feature"));

                }
            }

            this.statusConvergen = this.converge(oldCentroid,newCentroid);
            fs.delete(pathOldCentroid,true);
            writeAvroFileToHDFS("/output_2/"+this.configuration.get("name")+"/centroid-update-r-00000.avro"
            ,genericRecordList,schemaOld,fs);
        }
    }
    private void writeAvroFileToHDFS(String path, List<GenericRecord> list, Schema schema,FileSystem fileSystem) throws IOException {
        GenericDatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
        try(DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter)){
            dataFileWriter.setCodec(CodecFactory.snappyCodec());
            Path outputFileNewCentroid = new Path(  path);
            dataFileWriter.create(schema,fileSystem.create(outputFileNewCentroid));
            for(GenericRecord record : list){
                dataFileWriter.append(record);
            }
        }
    }
    public boolean converge(Map<Integer,Map<String,Double>>oldCentroid, Map<Integer,Map<String,Double>> newCentroid ){
        boolean isEqual =
                oldCentroid.entrySet().stream()
                        .allMatch(entry -> entry.getValue().equals(newCentroid.get(entry.getKey())));
        return isEqual;
    }


}
