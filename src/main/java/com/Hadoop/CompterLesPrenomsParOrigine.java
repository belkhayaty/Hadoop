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
			String ligne = valeur.toString(); // on traite une ligne donnee
			String[] colonnes = ligne.split(";"); // on recupere les colonne sen divisant la ligne par occurence de ;
			String[] origines = colonnes[2].trim().split(","); // on prend la colonne des origine qui vient en troisieme place puis on elimine les effets de bord avec tirm et on divise selon les , s il ya plusieurs origines
			this.nombreDOrigines.set(origines.length);  // on recupere le nombre d origine dans la ligne donnee
			output.collect(new Text(this.nombreDOrigines+ " origins"),unite); // puis on envoie cde nombre avec le nombre 1 pour dire qu il y a une occurence de x origines
		
		}
	}

	public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text clef, Iterator<IntWritable> valeurs, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			int somme = 0;
			while (valeurs.hasNext()) {
				somme += valeurs.next().get(); // on somme pour chaque valeur d x origines les 1 correspondants
			}
			output.collect(clef, new IntWritable(somme)); // puis on donne en sortie les nombres d'origines avec le nombre de leur occurences
		}
	}
}


