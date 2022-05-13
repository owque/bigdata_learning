package com.owen.bigdata.week2;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.stream.Collectors;

/**
 * MapReduceWeek2
 *
 * @author owque
 * @date 5/13/22
 */
public class FlowInfoMapReduceWeek2 {

    public static class FlowInfoArrayWrite extends ArrayWritable {
        public FlowInfoArrayWrite(int[] ints) {
            super(IntWritable.class);
            IntWritable[] intWritable = new IntWritable[ints.length];
            for (int i = 0; i < ints.length; i++) {
                intWritable[i] = new IntWritable(ints[i]);
            }
            set(intWritable);
        }

        @Override
        public IntWritable[] get() {
            Writable[] temp = super.get();
            IntWritable[] values = new IntWritable[temp.length];
            for (int i = 0; i < temp.length; i++) {
                values[i] = (IntWritable) temp[i];
            }
            return values;
        }

        @Override
        public String toString() {
            return Arrays.asList(this.get())
                    .stream()
                    .map(IntWritable::toString)
                    .collect(Collectors.joining(" "));
        }
    }

    public static class MapClass extends MapReduceBase implements Mapper<LongWritable, Text, Text, ArrayWritable> {
        @Override
        public void map(LongWritable key, Text value, OutputCollector<Text, ArrayWritable> output, Reporter reporter) throws IOException {
            if (StringUtils.isNotBlank(value.toString())) {
                String line = value.toString();
                String[] parts = line.split("	");
                int FIELDS_COUNT = 10;
                if (parts.length >= FIELDS_COUNT) {
                    int[] arr_int = new int[]{
                            Integer.parseInt(parts[8]),
                            Integer.parseInt(parts[9]),
                            Integer.parseInt(parts[9]) + Integer.parseInt(parts[8])};
                    output.collect(new Text(parts[1]), new FlowInfoArrayWrite(arr_int));
                }
            }
        }
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, FlowInfoArrayWrite, Text, FlowInfoArrayWrite> {
        @Override
        public void reduce(Text key, Iterator<FlowInfoArrayWrite> values, OutputCollector<Text, FlowInfoArrayWrite> output, Reporter reporter) throws IOException {
            int[] total = new int[3];
            while (values.hasNext()) {
                FlowInfoArrayWrite row = values.next();
                IntWritable[] flowInfo = row.get();
                for (int i = 0; i < flowInfo.length; i++) {
                    total[i] += flowInfo[i].get();
                }
            }
            output.collect(key, new FlowInfoArrayWrite(total));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        JobConf job = new JobConf(conf, FlowInfoMapReduceWeek2.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowInfoArrayWrite.class);
        job.setOutputFormat(TextOutputFormat.class);
        job.setInputFormat(TextInputFormat.class);

        job.setMapperClass(MapClass.class);
        job.setReducerClass(Reduce.class);

        job.setNumMapTasks(5);
        job.setNumReduceTasks(2);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setJobName("Flow_Calculation_MapReduce_Week2");

        JobClient.runJob(job);
    }
}