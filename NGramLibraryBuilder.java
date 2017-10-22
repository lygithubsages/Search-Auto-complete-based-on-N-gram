
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class NGramLibraryBuilder {
	public static class NGramMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		int noGram;
		@Override
		public void setup(Context context) {
		//setup stage, use configuration;
			Configuration configuration = context.getConfiguration();
			//need to set number of n-gram
            noGram = configuration.getInt("noGram", 5);

		}

		// map method
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			//this MapReduce's task is to build n-gram library, needs to split the sentence read by buffer from 2 to N;
			//因为这次不用在setup里建立HashMap, 所以不用bufferedReader读文件了，这次mapper所需的读进来的内容再value当中。
			String line = value.toString();

			line.toLowerCase().trim();
			line.replaceAll("[^a-z]", " ");

            String[] words = line.split("\\s+");
            if (words.length < 2) {
                return;
            }
            //eg. i , love, big, data;
            for (int i = 0; i < words.length; i++) {
                StringBuilder sb = new StringBuilder();
                sb.append(words[i]);
                for (int j = 1; i + j < words.length && j < noGram; j++) {
                    sb.append(" ");
                    sb.append(words[i + j]);
                    //加后马上写出， 因为2-n gram 都需要， 这个循环写从i开始的 2-n gram
                    context.write(new Text(sb.toString().trim()), new IntWritable(1));

                }
            }
		}
	}

	public static class NGramReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		// reduce method
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
		    int sum = 0;
		    for (IntWritable value: values) {
		        sum =  sum + value.get();
            }
            context.write(key, new IntWritable(sum));
		}
	}
}

