import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class NGramLibraryBuilder {
	public static class NGramMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		int noGram;
		@Override
		public void setup(Context context) {
			//how to get n-gram from command line?
		}

		// map method
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			//nGram
			Configuration conf = context.getConfiguration();
			int nGram = conf.getInt("nGram", 5);

			String sentence = value.toString().toLowerCase().trim();
			sentence = sentence.replace("[^a-z]"," ");

			String[] words = sentence.split("\\s+");

			for(int i = 0; i < words.length - 1; i++) {
				StringBuilder sb = new StringBuilder();
				sb.append(words[i]);
				for(int j = 1; j < words.length && j < nGram; j++) {
					sb.append(" ");
					sb.append(words[i]);
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
			for(IntWritable count: values) {
				sum += count.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}

}