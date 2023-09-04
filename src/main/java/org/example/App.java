package org.example;

import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hc.client5.http.classic.methods.HttpPut;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.log4j.Logger;
import org.example.clustering.JobClustering;
import org.example.extract_feature.JobExtractFeature;
import org.example.extract_transform.JobExtractTransform;

import java.util.HashSet;
import java.util.List;
import java.util.Map;

/**
 * Hello world!
 *
 */
public class App extends Configured implements Tool
{
    private static Logger logger = Logger.getLogger(App.class);
    public static void main( String[] args ) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(),new App(), args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] strings) throws Exception {
        Options options = new Options();
        options.addOption("i","input",true,"Input File");
        options.addOption("o","output",true,"Output File");
        options.addOption("a",true,"Option Apllicaiton To Run");
        options.addOption("k", true, "Number Cluster");
        options.addOption("m",true,"Maximum Iteration");
        options.addOption("name",true,"Name File");

        CommandLineParser parser = new PosixParser();
        CommandLine cmd = null;

        boolean success;
        try {
            cmd=  parser.parse(options,strings);
            String inputFolder = cmd.getOptionValue("input");
            String outputFolder = cmd.getOptionValue("output");
            String subSystemToRun = cmd.getOptionValue("a");
            Configuration configuration = getConf();
            configuration.set("fs.defaultFS","hdfs://master:9000");
            configuration.set("mapreduce.framework.name","yarn");
            configuration.set("mapreduce.map.output.compress", "true");
            configuration.set("mapreduce.map.output.compress.codec", SnappyCodec.class.getName());
            configuration.set("mapreduce.map.memory.mb","3072");
            configuration.set("mapreduce.reduce.memory.mb","3072");

            JobExtractTransform jobExtractTransform = new JobExtractTransform(inputFolder,outputFolder,configuration);
            if(subSystemToRun.equals("1")) {
                try (Job firstJob = jobExtractTransform.runJob()) {
                    success = firstJob.waitForCompletion(true);
                    if (success) {
                        try (CloseableHttpClient closeableHttpClient = HttpClientBuilder.create().build()) {
                            String ip = "192.168.88.2";
                            String port = "14000";
                            String output = outputFolder + "/jumlah_dokumen.txt";
                            String url = "http://" + ip + ":" + port + "/webhdfs/v1" + output +
                                    "?op=CREATE&" +
                                    "overwrite=true&" +
                                    "user.name=hadoopuser";
                            HttpPut httpPut = new HttpPut(url);
                            try (StringEntity stringEntity = new StringEntity(
                                    String.valueOf(firstJob.getCounters().findCounter(CountersEnum.TOTAL_MAP_RECORDS).getValue())
                                    , ContentType.APPLICATION_OCTET_STREAM)) {
                                httpPut.setEntity(stringEntity);
                            }
                            closeableHttpClient.execute(httpPut, respose -> {
                                HttpEntity entity = respose.getEntity();
                                String result = EntityUtils.toString(entity);
                                logger.info(result);
                                EntityUtils.consume(entity);
                                return respose;
                            });
                        }
                    }
                }
            }else if(subSystemToRun.equals("2")){
                System.out.println("Sub System Extract Feature");
                String cluster = cmd.getOptionValue("k");
                logger.info(cluster);
                configuration.set("k",cluster);
                String name =  cmd.getOptionValue("name");
                configuration.set("name",name);
               logger.info(name);

                JobExtractFeature jobExtractFeature = new JobExtractFeature(
                        "/output/"+name+"/part-r-00000.avro",
                        "/output_1/"+name,
                        configuration
                );
                jobExtractFeature.runJobFirst();
                //String output = "/jumlah_kata.txt";
            }else if(subSystemToRun.equals("3")){
                System.out.println("System To Run Clustering KMeans");
                String name = cmd.getOptionValue("name");
                configuration.set("name",name);
                logger.info(name);
                String max = cmd.getOptionValue("m");
                configuration.set("m",max);
                logger.info(max);

                int maxInteger = Integer.parseInt(max);
                int count = 1;

                while(true){
                    if(count == maxInteger){
                        break;
                    }
                    logger.info("ITERATION : " + count);
                    long startTime = System.currentTimeMillis();
                    JobClustering jobClustering = new JobClustering(
                            "/output_2/"+configuration.get("name")+"/part-r-00000.avro",
                            "/output_3/"+configuration.get("name"),
                            configuration,
                            count
                    );
                    jobClustering.runJobClustering();
                    logger.info("SEE :" + jobClustering.getSse());
                    if(jobClustering.isStatusConvergen()){
                        long endTime = System.currentTimeMillis();
                        logger.info("TIME : " + (endTime-startTime)/ 1000.0);
                        logger.info("CONVERGE ");
                        break;
                    }
                    long endTime = System.currentTimeMillis();
                    logger.info("TIME : " + (endTime-startTime)/ 1000.0);
                    count = count +1;
                }



            }
        } catch (ParseException e) {
            System.err.println("Error parsing command line arguments: " + e.getMessage());
            printHelp(options);
            return 1;
        }
        return 1;
    }
    private static boolean converge(Map<Integer,List<Integer>> old, Map<Integer,List<Integer>> newGroup){
        for(Map.Entry<Integer,List<Integer>> entry : old.entrySet()){
            Integer key = entry.getKey();
            if(!newGroup.containsKey(key)){
                return false;
            }
            if(!new HashSet<>(entry.getValue()).containsAll(newGroup.get(key))){
                return false;
            }
        }
        return true;
    }
    private void printHelp(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("Main", options, true);
    }
}
