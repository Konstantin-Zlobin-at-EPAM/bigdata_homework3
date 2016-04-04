package com.epam.bigdata.visitsspends;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;

public class VSByIPCountTool extends Configured implements Tool {

    final static Logger LOG = LoggerFactory.getLogger(VSByIPCountTool.class);

    public static void main(String[] args) {
        try {
            int res = ToolRunner.run(new Configuration(), new VSByIPCountTool(), args);
            System.exit(res);
        } catch (Exception ex) {
            LOG.error("ERROR: ", ex);
            System.exit(1);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "visits_and_spends_by_ip");

        conf.set("mapreduce.map.output.compress", "true");
        conf.set("mapreduce.map.output.compress.codec", "org.apache.hadoop.io.compress.SnappyCodec");

        job.setJarByClass(VSByIPCountTool.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(VSWritable.class);

        job.setMapperClass(VSMapper.class);
        job.setCombinerClass(VSReducer.class);
        job.setReducerClass(VSReducer.class);

        job.setInputFormatClass(TextInputFormat.class);

        if (args.length > 1 && args[1].endsWith(".bin")) {
            SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);
            SequenceFileOutputFormat.setCompressOutput(job, true);
            conf.set("mapreduce.output.fileoutputformat.compress.codec", "org.apache.hadoop.io.compress.SnappyCodec");
        } else {
            job.setOutputFormatClass(TextOutputFormat.class);
        }

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1] + "_" + System.currentTimeMillis()));

        int returnCode = job.waitForCompletion(true) ? 0 : 1;
        LOG.info("Custom counters:");
        LOG.info("\t" + BrowserCounterGroup.class.getSimpleName());
        Arrays.asList(BrowserCounterGroup.values()).stream().forEach((BrowserCounterGroup counter) -> {
            try {
                LOG.info("\t\t" + counter.name() + "=" + job.getCounters().findCounter(counter).getValue());
            } catch (IOException e) {
                LOG.error("cannot get counter: " + counter, e);
                throw new RuntimeException(e);
            }
        });
        return returnCode;
    }

}
