package com.Hadoop;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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

public class CompterLesMalesEtLesFemelles{

	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(CompterLesMalesEtLesFemelles.class);
		conf.setJobName("compterLesMalesEtLesFemelles");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(DoubleWritable.class);

		conf.setMapperClass(Map.class);
		conf.setReducerClass(Reduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		JobClient.runJob(conf);
	}

	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
		private static final Text mot = new Text("percentage des mâles");
		private IntWritable estMale = new IntWritable(0);
		public void map(LongWritable clef, Text valeur, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			String ligne = valeur.toString(); // traiter la ligne du csv
			String[] colonnes = ligne.split(";"); // on la divise en colonnes (delimitees par un ;)
			String[] genres = colonnes[1].split(","); // on prend la deuxieme colonne qui contient le genre
			for (int i=0; i<genres.length; i++){
				int estMale = 0;
				if (genres[i].trim().equals("m"))  { // on choisit de considerer male des qu il y a un m dans la colonne de genres
					estMale= 1;
				}
				this.estMale.set(estMale);
				output.collect(mot, this.estMale); // si le prenom est male on l ajoute avec un 1 sinon avec un 0
			}

		}
	}

	public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, DoubleWritable> {
		public void reduce(Text clef, Iterator<IntWritable> valeurs, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
			int males = 0;
			int total = 0;
			while (valeurs.hasNext()) {
				males += valeurs.next().get(); // on compte le nombre de males traites
				total += 1;  // on compte le nombre de personnes traitees
			}
			output.collect(clef, new DoubleWritable((double)(males)/(double)(total))); // on donne en sortie la proportion des males sur toute la population
		}
	}
}
