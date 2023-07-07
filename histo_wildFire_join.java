//mapper 1: joining data files 
//wildfire data labelling

//This class used to label the values in the output file from job2 (HistoricalFires). 
//This process is important for joining
package org.myorg;

//Importing the libraries that will be used in Mapper Class 
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

//exiend WeatherLocationFilterMApper from Mapper Class in hadoop 
public class histo_wildFire_join extends Mapper<LongWritable, Text, Text, Text>{
	
	 @Override
	 
	 public void map(LongWritable key, Text value, Context context)
	 throws IOException, InterruptedException
	 {
		 // split the line on tab because the (key,value) pairs are separated by tab 
		 String[] record = value.toString().split("\t");
		 //extraxt the same values from the input file and label them with "F:" at the begining 
		 context.write(new Text(record[0]), new Text("F:" + record[1]));	    
	 }	 	
	
}


