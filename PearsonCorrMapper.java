// the class is used to map each two variables indices with the value in the those indices in each row.
package org.myorg;

//Importing the libraries that will be used in Mapper Class 
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PearsonCorrMapper extends Mapper<LongWritable, Text, Text, Text>{
	 @Override
	  
	 public void map(LongWritable key, Text value, Context context)
	 throws IOException, InterruptedException
	 {
		 //split the keys and values of the last mapreduce job output on tab
		 String[] arr = value.toString().split("\t");
		 //extract the values only, and split them on comma,then save it in string list
		 String[] measuredValues = arr[1].split(",");
		 //loop through the values to extract the number of induces and the values of the indices
		 //number of variables(indices)
		  int size = measuredValues.length;
		  //loop through for the first index
		  for(int i=0; i < size -1; i++) {
			  //loop through for the second index
			  for(int j=i+1; j < size; j++) {
				  //save the indices as a key
				  String outputKey = String.valueOf(i) + "," + String.valueOf(j);
				  //save the measurements in the indices as a value
				  String outputValue = measuredValues[i] + "," + measuredValues[j];
				  //emit the results
				  context.write(new Text(outputKey), new Text (outputValue));
			  }
		  }	    
	 }	
}