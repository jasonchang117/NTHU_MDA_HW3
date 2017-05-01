package org.apache.hadoop.examples;

import java.io.*;
import java.io.OutputStreamWriter;
import java.util.*;
import java.net.*;
import java.lang.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class Kmeans{
	public static final int dim = 58;
	public static final int num_centroid = 10;

	public static class Map extends Mapper<Object, Text, Text, Text>
	{	
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			Configuration conf = context.getConfiguration();
			String[] line = value.toString().split(" ");
			double[][] centroids = new double[num_centroid][dim];
			double[] point = new double[dim];
			Text reduce_key = new Text();
			Text reduce_val = new Text();
			
			for(int i=0;i<num_centroid;i++){
				String[] getconf = conf.getStrings("C_"+Integer.toString(i+1));
				String[] temp = getconf[0].split(" ");

				for(int j=0;j<dim;j++){
					centroids[i][j] = Double.parseDouble(temp[j]);
				}
			}
		
			for(int i=0;i<dim;i++){
				point[i] = Double.parseDouble(line[i]);	
			}

			double[] temp = new double[num_centroid];
			for(int i=0;i<num_centroid;i++){
				for(int j=0;j<dim;j++){
					// temp[i] += Math.pow((point[j] - centroids[i][j]), 2.0);			// for Euclidean distance
					temp[i] += Math.abs(point[j] - centroids[i][j]);					// for Manhattan distance
				}
				// temp[i] = Math.sqrt(temp[i]);			// for Euclidean distance
			}
		
			double ref = 10000000.0;
			int idx = 0;
			for(int i=0;i<num_centroid;i++){
				if(temp[i] < ref){
					ref = temp[i];
					idx = i;
				}
			}
			
			// ref = Math.pow(ref, 2.0);			// for Manhattan

			reduce_key.set( Integer.toString(idx) );
			reduce_val.set( value.toString() + " " + Double.toString(ref) );
			context.write(reduce_key, reduce_val);
		}
	}
	
	public static class Reduce extends Reducer<Text, Text, Text, Text>
	{
		MultipleOutputs<Text, Text> mos;
		
		public void setup(Context context){
			mos = new MultipleOutputs(context);
		}

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			Configuration conf = context.getConfiguration();
			String input_key = key.toString();
			String output_line = "";
			double[] total_values = new double[dim];	
			double cost = 0.0;

			int count = 0;										// count for how many points are in this reducer(cluster)
			for(Text val : values){
				String[] line = val.toString().split(" ");

				for(int i=0;i<dim;i++){
					total_values[i] += Double.parseDouble(line[i]);
				}
				cost += Double.parseDouble(line[dim]);
				count += 1;
			}
			
			for(int i=0;i<dim;i++){
				total_values[i] = total_values[i]/(double)count;
				output_line += Double.toString(total_values[i]) + " ";
			}

			mos.write("centroid", NullWritable.get(), new Text(output_line));
			mos.write("cost", NullWritable.get(), new Text(Double.toString(cost)));
		}

		protected void cleanup(Context context) throws IOException, InterruptedException{
			mos.close();
		}
	}
	
	public static void run(String[] input_centroids) throws Exception 
	{
		Configuration conf = new Configuration();
		
		for(int i=0;i<num_centroid;i++){
			conf.setStrings("C_" + Integer.toString(i+1), input_centroids[i]);
		}
		
		conf.set("centroid", "centroid");
		Job job = new Job(conf, "Kmeans");

		job.setJarByClass(Kmeans.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		
		MultipleInputs.addInputPath(job, new Path("/user/root/data/HW3/data.txt"), TextInputFormat.class, Map.class);
				
		job.setOutputFormatClass(TextOutputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path("/user/root/output/kmeans/temp"));

		MultipleOutputs.addNamedOutput(job, "centroid", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "cost", TextOutputFormat.class, NullWritable.class, Text.class);
		
		job.waitForCompletion(true);
    }
}
