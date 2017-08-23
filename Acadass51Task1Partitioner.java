package mapreduce.demo.task2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;



public class Acadass51Task1Partitioner extends Partitioner<Text,IntWritable> {

	@Override
    public int getPartition(Text key, IntWritable value, int numReduceTasks) {
		
		String name = key.toString();
		//processing to different reducers
		if (name.matches("^[A-I].*$")){	
			return 1;
		} else if (name.matches("^[G-L].*$")) {
			return 2;
		} else if (name.matches("^[M-R].*$")) {
			return 3;
		} else {
			return 0;
		}		
    }
}

