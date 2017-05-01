package org.apache.hadoop.examples;

import java.io.*;
import java.io.OutputStreamWriter;
import java.util.*;
import java.net.*;
import java.text.DecimalFormat;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.Path;

public class KmeansJob
{
	private static int dim = 58;

	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();	// c1.txt   data.txt
		String [] centroids = new String[10];
		String p = "";

		for(int n=0;n<20;n++)
		{
			System.out.println("n:" + n);
			try
			{
				if(n == 0)
					p = "/user/root/data/HW3/c1.txt";
				else
					p = "/user/root/output/kmeans/temp/centroid-r-00000";
				
				Path path = new Path(p);
				FileSystem fs = FileSystem.get( new Configuration() );
				BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
				String inputline = br.readLine();
			
				int i = 0;
				while(inputline != null)
				{
					centroids[i] = inputline;		
					i += 1;
					inputline = br.readLine();
				}
			}
			catch(Exception e)
			{
				e.printStackTrace();
			}
			
			FileSystem f = FileSystem.get( new Configuration() );
			if( f.exists(new Path("/user/root/output/kmeans/temp")) )
				f.delete(new Path("/user/root/output/kmeans/temp"), true);
		
			Kmeans.run(centroids);		// run MapReduce
			
			compute_cost(n);
		}
	}
	
	public static void compute_cost(int n)throws Exception
	{
		try
		{
			DecimalFormat df = new DecimalFormat("##.00000");
			Path path = new Path("/user/root/output/kmeans/temp/cost-r-00000");
			FileSystem fs = FileSystem.get( new Configuration() );
			BufferedReader br = new BufferedReader( new InputStreamReader(fs.open(path)) );
			String line = br.readLine();
			double cost = 0.0;

			while(line != null)
			{
				cost += Double.parseDouble(df.format(Double.parseDouble(line)));		
				line = br.readLine();
			}

			Path p = new Path("/user/root/output/kmeans/cost_" + Integer.toString(n) + ".txt");
			FileSystem f = FileSystem.get( new Configuration() );
			BufferedWriter bw = new BufferedWriter( new OutputStreamWriter(f.create(p, true), "UTF-8"));
			String write_line = Double.toString(cost) + "\n";
			bw.write(write_line);
			bw.flush();
			bw.close();
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}	
}
