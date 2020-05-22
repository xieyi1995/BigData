package BigData.MapReduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class WordCountReduce extends Reducer<Text, IntWritable,Text,IntWritable> {
    IntWritable result = new IntWritable();
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        Iterator<IntWritable> interator = values.iterator();
        for(IntWritable value:values){
            sum+=value.get();
        }
        result.set(sum);
        context.write(key,result);
    }
}
