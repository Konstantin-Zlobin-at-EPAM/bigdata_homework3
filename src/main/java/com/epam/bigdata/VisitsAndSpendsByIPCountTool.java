package com.epam.bigdata;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class VisitsAndSpendsByIPCountTool extends Configured implements Tool {

    final static Logger LOG = LoggerFactory.getLogger(VisitsAndSpendsByIPCountTool.class);

    public static void main(String[] args) {
        try {
            int res = ToolRunner.run(new Configuration(), new VisitsAndSpendsByIPCountTool(), args);
            System.exit(res);
        } catch (Exception ex) {
            LOG.error("ERROR: ", ex);
            System.exit(1);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();

        conf.set("mapreduce.map.output.compress", "true");
        conf.set("mapreduce.map.output.compress.codec", "org.apache.hadoop.io.compress.SnappyCodec");

        Job job = Job.getInstance(conf, "visits_and_spends_by_ip");

        job.setJarByClass(VisitsAndSpendsByIPCountTool.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(VisitsAndSpends.class);

        job.setMapperClass(VisitsAndSpendsByIPCountTool.Mapper.class);
        job.setCombinerClass(VisitsAndSpendsByIPCountTool.Reducer.class);
        job.setReducerClass(VisitsAndSpendsByIPCountTool.Reducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1] + "_" + System.currentTimeMillis()));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class Mapper extends org.apache.hadoop.mapreduce.Mapper<
            LongWritable, Text,
            Text, VisitsAndSpends> {
        final static Logger LOG = LoggerFactory.getLogger(VisitsAndSpendsByIPCountTool.Mapper.class);
        final static int IP = 4;
        final static int BIDDING_PRICE = 18;

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            final String line = value.toString();
            final String[] columnValues = line.split("\\t");
            final String ip = columnValues[IP];
            Long biddingPrice;
            try {
                biddingPrice = Long.parseLong(columnValues[BIDDING_PRICE]);
            } catch (NumberFormatException nfe) {
                biddingPrice = 0L;
            }
            context.write(new Text(ip), new VisitsAndSpends(1L, biddingPrice));
        }
    }

    public static class Reducer extends org.apache.hadoop.mapreduce.Reducer<
            Text, VisitsAndSpends,
            Text, VisitsAndSpends> {
        final static Logger LOG = LoggerFactory.getLogger(VisitsAndSpendsByIPCountTool.Reducer.class);

        @Override
        protected void reduce(Text key, Iterable<VisitsAndSpends> values, Context context)
                throws IOException, InterruptedException {
            long sumVisits = 0;
            long sumSpends = 0;
            for (VisitsAndSpends val : values) {
                sumVisits += val.getVisits();
                sumSpends += val.getSpends();
            }
            context.write(key, new VisitsAndSpends(sumVisits, sumSpends));
        }
    }

    public static class VisitsAndSpends implements Writable {
        private long visits;
        private long spends;

        public VisitsAndSpends() {
        }

        public VisitsAndSpends(long visits, long spends) {
            this.visits = visits;
            this.spends = spends;
        }

        public long getVisits() {
            return visits;
        }

        public void setVisits(long visits) {
            this.visits = visits;
        }

        public long getSpends() {
            return spends;
        }

        public void setSpends(long spends) {
            this.spends = spends;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeLong(visits);
            out.writeChar('\t');
            out.writeLong(spends);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            visits = in.readLong();
            in.readChar();
            spends = in.readLong();
        }

        public static VisitsAndSpends read(DataInput in) throws IOException {
            final VisitsAndSpends result = new VisitsAndSpends();
            result.readFields(in);
            return result;
        }

        @Override
        public String toString() {
            return "visits: " + visits + ", " + "spends: " + spends;
        }
    }
}