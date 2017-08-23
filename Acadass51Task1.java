package mapreduce.demo.task2;



import mapreduce.demo.task1.Acadass42Task1Mapper;
import mapreduce.demo.task1.Acadass42Task1Reducer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Acadass51Task1 {
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "Assign611");
		job.setJarByClass(Acadass51Task1.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		  // Specify the number of reducer to 1
	    job.setNumReduceTasks(1);
	    
	    // Set the partitioner
	    job.setPartitionerClass(Acadass51Task1Partitioner.class);
		
		job.setMapperClass(Acadass51Task1Mapper.class);
		job.setReducerClass(Acadass51Task1Reducer.class);
		
		//Uncomment for Assignment 5.1.2 where u use combiner
		//job.setCombinerClass(Acadass51Task1Reducer.class);
		 
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0])); 
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		
		/*
		Path out=new Path(args[1]);
		out.getFileSystem(conf).delete(out);
		*/
		
		job.waitForCompletion(true);
	}
}
