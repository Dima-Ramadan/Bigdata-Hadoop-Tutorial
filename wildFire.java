package org.myorg;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;


import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.lib.chain.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;


public class wildFire
{

	public static void main(String[] args) throws Exception
	{
		//check if the user entered 6 arguments in the propmpt
		if (args.length != 6)
		{
			System.err.println("Usage: wildFire <input path1> <input path2> <Joining Output> <Correlation output> <Region> ");
			System.exit(-1);
		}
		

	/* First mapreduce job ("weather") , will be applied on Historical Weather */
		Configuration conf = new Configuration();
		// the 5th input in the command line will filter the region to analyse
		conf.set("LocationFilter", args[5]);
		//initialize the first job object job1
		Job job1;
		//set the job name (will be used in JobControl)
		job1=Job.getInstance(conf, "weather");
		//set the driver class
		job1.setJarByClass(wildFire.class);
		//job1.setNumReduceTasks(0);  /*use this command to see the output of the mappers only*/
		
		// The input path for HistoricalWeather.txt file (the second input in the command line 
		FileInputFormat.addInputPath(job1, new Path(args[1]));   //The input of the first job
		//set a fixed output file for the first mapreduce job (will be used in the third job as input path)
		FileOutputFormat.setOutputPath(job1, new Path("job1_output")); 
		job1.setOutputKeyClass(Text.class); //set the out type for the key
		job1.setOutputValueClass(Text.class); //set the output type for the value

		//First Mapper for Historical Weather
		Configuration WeatherLocationFilterMapperConf = new Configuration(false); //change the default mapper configuration
		//ChainMapper.add Mapper() used to chain the mappers in the order they are written in.
		ChainMapper.addMapper(job1,    //set the job the mapper belongs to 
				WeatherLocationFilterMApper.class,   //the name of this mapper class
				LongWritable.class, //input key type
				Text.class,  //input value type
				Text.class,  //output key type
				Text.class,  //output value type
				WeatherLocationFilterMapperConf); //set the configuration 

		//Second Mapper for Historical Weather 
		Configuration WeatherDateMapperConf = new Configuration(false); //change the default mapper configuration
		ChainMapper.addMapper(job1,  //set the job the mapper belongs to 
				WeatherDateMapper.class, //the name of this mapper class
				Text.class, //input key type
				Text.class, //input value type
				Text.class, //output key type
				DoubleWritable.class,  //output value type
				WeatherDateMapperConf); //set the configuration 
		
		//Reducer for Historical Weather
		Configuration WeatherReducerConf = new Configuration(false); //change the default mapper configuration
		ChainReducer.setReducer(job1, // ChainReducer.setReducer() used to chain the reducer in the multi-step job 
				WeatherReducer.class,  //the name of the reducer class
				Text.class,		//input key type
				DoubleWritable.class, //input value type
				Text.class,  //output key type
				DoubleWritable.class,  //output value type
				WeatherReducerConf);  //set the configurations.
		
		
		
		
	
		/* Second map reduce job, used for processing historical wildfire file */
		Configuration conf2 = new Configuration();
		conf2.set("LocationFilter", args[5]);//the 5th input in the command line will filter the region to analyse
		Job job2; ////initialize the first job object job1
		job2=Job.getInstance(conf2, "HistoricalFires"); //set the job's name
		job2.setJarByClass(wildFire.class); //set driver class name
		//job2.setNumReduceTasks(0);  // used if we want to see the mappers' output

		FileInputFormat.addInputPath(job2, new Path(args[2])); //The input of the second mapreduce job
		FileOutputFormat.setOutputPath(job2, new Path("job2_output")); //output directory of job2
		
		job2.setOutputKeyClass(Text.class); //set the output key type
		job2.setOutputValueClass(Text.class); //set the output value type

		//First Mapper for Historical wildfires
		Configuration HistoricalFiresMapperConf = new Configuration(false); //change the default settings
		ChainMapper.addMapper(job2, 
				HistoricalFiresMapper.class, //the mapper class used
				LongWritable.class, //input key type
				Text.class, //input value type
				Text.class, //output key type
				Text.class, //output value type
				HistoricalFiresMapperConf ); //set the configuration
		
		
		//Second Mapper for Historical wildfires
		Configuration FiresDateMapperConf = new Configuration(false); 
		ChainMapper.addMapper(job2,
				HistoricalFiresDateMapper.class, //the mapper class used
				Text.class,   //input key type
				Text.class,  //input value type
				Text.class,  //output key type
				Text.class, //output value type
				FiresDateMapperConf);//set the configuration
		
			
		//Reducer for Historical Wildfire	
		Configuration HistoricalWildFiresReducerConf = new Configuration(false);
		ChainReducer.setReducer(job2,
				HistoricalWildfiresReducer.class, //the reducer class used
				Text.class,	  //input key type
				Text.class,  //input value type
				Text.class,  //output key type
				Text.class, //output value type
				HistoricalWildFiresReducerConf);//set the configuration
		
		
		
	
		
		/* Third map reduce job, used to Join the resultant data files from the previous map reduce jobs  */
		Configuration conf3 = new Configuration(); 
		Job job3;
		job3=Job.getInstance(conf3, "Joining"); //set the job name
		job3.setJarByClass(wildFire.class);  //set the driver class
		job3.setReducerClass(JoiningReducer.class); //set reducer class
		//job3.setNumReduceTasks(0); // this code is used to view the mappers output
		
		// MultipleInputs.addInputPath() is used to allow multiple input files for the mapreduce job 
		//it takes (jobName, inputPath, inputDataType, ClassName)
		 MultipleInputs.addInputPath(job3, new Path("job1_output"),TextInputFormat.class, Join_weather_mapper.class);
		 MultipleInputs.addInputPath(job3, new Path("job2_output"),TextInputFormat.class, histo_wildFire_join.class);
		 
		 //set the output path in the prompt (third argument)
		 FileOutputFormat.setOutputPath(job3, new Path(args[3])); //output of job3
		//Data types of the output key and value 
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(Text.class);

	
		
		/* 4th map reduce job, used for finding Pearson's Correlation Matrix */
		Configuration conf4 = new Configuration();
		Job job4;  //set the job
		job4=Job.getInstance(conf4, "CorrelationMatrix");//set the job name
		job4.setJarByClass(wildFire.class);// set the driver class
		//job4.setNumReduceTasks(0); //use this code if you want to view the mapper output only

		FileInputFormat.addInputPath(job4, new Path(args[3])); //The input of the fourth job
		FileOutputFormat.setOutputPath(job4, new Path(args[4])); //output of the fourth class
		
		job4.setOutputKeyClass(Text.class); //set key output data types
		job4.setOutputValueClass(Text.class); //set the output value data type

		//First Mapper for CorrelationMatrix
		
		//the default settings can be used here also 
		Configuration CorrConf = new Configuration(false);
		ChainMapper.addMapper(job4, 
				PearsonCorrMapper.class, //set the class 
				LongWritable.class, //input data type
				Text.class, //input data type(value)
				Text.class, //output data type (key
				Text.class, //output data type (value)
				CorrConf ); 
		
		
//		//Reducer for CorrelationMatrix
		Configuration CorrReducerConf = new Configuration(false);
		ChainReducer.setReducer(job4,
				PearsonCorrReducer.class, //set the class
				Text.class, //input data type
				Text.class, //input data type(value)
				Text.class, //output data type (key
				DoubleWritable.class, //output data type (value)
				CorrReducerConf);
		
		
		
		
	
		
		
		//JOBS CONTROL AND CHAINING
		//declare a new Job Control Object
		JobControl jobControl = new JobControl("wildFire_MRJobs");
		
		//ControlledJob for the first mapreduce job, with no dependencies
		ControlledJob weather = new ControlledJob(job1, null);
		//ControlledJob for the second mapreduce job, with no dependencies
		ControlledJob fires = new ControlledJob(job2, null);
		
		//intialize an ArrayList, will contain the dependencies for the third job 
		ArrayList<ControlledJob> job3dependencies = new ArrayList<ControlledJob>();
		job3dependencies.add(weather);
		job3dependencies.add(fires);
		//Controlled job for the third job with dependencies(job1, job2)
		ControlledJob Joining = new ControlledJob(job3, job3dependencies);
		
		//intialize an ArrayList, will contain the dependencies for the fourth job 
		ArrayList<ControlledJob> Job4dependencies = new ArrayList<ControlledJob>();
		Job4dependencies.add(Joining);
		//Controlled job for the fourth job with dependencies(job3)
		ControlledJob Correlation = new ControlledJob(job4, Job4dependencies);
		
		//add the ControlledJobs to JobControl chain. 
		jobControl.addJob(weather);
		jobControl.addJob(fires);
		jobControl.addJob(Joining);
		jobControl.addJob(Correlation);

		
		// Running the Map reduce jobs chain using Thread method
		Thread runJobControl = new Thread(jobControl);
		//start the program
		runJobControl.start();
		//check if all the jobs finished their run, if yes it will exit, if no it will type (code is running)
		while (!jobControl.allFinished()) {
			System.out.println("Code is Running ...");
			Thread.sleep(1000);};
			int code = (jobControl.getFailedJobList().size() == 0 ? 0 : 1);
			Thread.sleep(100);

		System.exit(code);

	}
}



