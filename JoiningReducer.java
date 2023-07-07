//Joining the outputs from two mappers to one data file

package org.myorg;

//Import the libraries
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
public class JoiningReducer extends Reducer<Text, Text, Text, Text> {
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,
	InterruptedException {
		
	//initialize an empty String Temp, will be used to save the values from weather file
	//Initialize an empty String Fire, will be used to store the values from the wildfire file 	
	 String Temp = "";
	String Fire = "";
	//loop through the values of each key (values from weather wildfire taken from the output of previous 
	//mapreduce jobs files)
    for(Text value : values) {
    	//convert each value to string and check if it starts with T
    	//If yes split on ":" and save the second item (value) in Temp (data comes from weather data)
    	//if not split the value on ":" and save the second item in Fire (data comes from wildfire data)
        if (value.toString().startsWith("T")) {
            Temp = value.toString().split(":")[1];
        } else {
            Fire = value.toString().split(":")[1];
        }
    }
    //concatenate the Temp and Fire together 
    //The order of the variables  will be [ Temperature, FireArea, FireBrightness ]
    String merge = Temp + "," + Fire;
    context.write(key, new Text(merge)); //[Text, Text]
	
	}	
}


