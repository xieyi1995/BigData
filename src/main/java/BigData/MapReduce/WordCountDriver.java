package BigData.MapReduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordCountDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        long start = System.currentTimeMillis();

        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(WordCountDriver.class);
        job.setMapperClass(WordCountMap.class);
        job.setReducerClass(WordCountReduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job, new Path("E:\\input1"));
        FileOutputFormat.setOutputPath(job, new Path("E:\\outputWc1"));

        boolean b = job.waitForCompletion(true);

        long stop = System.currentTimeMillis();
        System.out.println("calculate time"+(stop-start));

        System.exit((b?0:1));
    }


}
