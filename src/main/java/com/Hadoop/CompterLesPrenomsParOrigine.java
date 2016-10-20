package com.Hadoop;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class CompterLesPrenomsParOrigine{

	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(CompterLesPrenomsParOrigine.class);
		conf.setJobName("compterLesPrénomsParOrigine");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		JobClient.runJob(conf);
	}

	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable unite = new IntWritable(1);
		private IntWritable nombreDOrigines = new IntWritable(1);

		public void map(LongWritable clef, Text valeur, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			String ligne = valeur.toString();
			String[] colonnes = ligne.split(";");
			String[] origines = colonnes[2].trim().split(",");
			this.nombreDOrigines.set(origines.length); 
			output.collect(new Text(this.nombreDOrigines+ " origins"),unite);
		
		}
	}

	public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text clef, Iterator<IntWritable> valeurs, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			int somme = 0;
			while (valeurs.hasNext()) {
				somme += valeurs.next().get();
			}
			output.collect(clef, new IntWritable(somme));
		}
	}
}


