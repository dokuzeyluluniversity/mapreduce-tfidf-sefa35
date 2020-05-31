package mriv;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.StringTokenizer;

import javax.print.attribute.standard.JobName;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.sun.tools.javac.util.List;



public class InvertedIndex {
	
	
	
	public static HashMap<String, Integer> stop_words = new HashMap<String, Integer>();
	public static HashMap<String, ArrayList<String>> table = new HashMap<String, ArrayList<String>>();
	public static HashMap<String, ArrayList<String>> table1 = new HashMap<String, ArrayList<String>>();

	//public static HashMap<String, Integer> t2 = new HashMap<String, Integer>();
	public static int jobNumber = 0;
	
	public static class InvertedIndexMapper extends 
	Mapper<Object,Text,Object,Text>{
	private Text keyInfo = new Text();//store the combination of word and URI
	private  Text valueInfo = new Text();//store the word frequency
	private FileSplit split;//store the split target
	
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
	
			
			createStopWordList("C:\\hadoop\\tr-stopword-list.txt");
			split = (FileSplit)context.getInputSplit();
	         
			String line = value.toString().replaceAll("[^a-zA-ZçğıöşüÇĞİÖŞÜ0-9]+", " ");
			String[] words = line.split(" ");
			for(String word:words) {
				if(word.contains("\t")) {
					String[] spell = word.split("\t");
					for (String s : spell) {
						if(s.length()>2 && !stop_words.containsKey(s)) {
							keyInfo.set(s.toLowerCase()+">"+split.getPath().getName().toString());
							valueInfo.set("1");
							context.write(keyInfo,valueInfo);
							//Output = (word>path,n)
						}
					}
				}
				else if(word.length()>2 && !stop_words.containsKey(word)) {
					keyInfo.set(word.toLowerCase()+">"+split.getPath().getName().toString());
					valueInfo.set("1");
					context.write(keyInfo, valueInfo);
					//Output = (word>path,n)
				}
			}
		
	}
}
	public static class InvertedIndexMapper1 extends 
	Mapper<Object,Text,Object,Text>{
	private Text keyInfo = new Text();
	private  Text valueInfo = new Text();
	
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		Text txt = new Text();
		String[] arr = value.toString().split(";");
		String[] arr1 = arr[0].split("\t");
		keyInfo.set(arr1[0]);
		valueInfo.set(arr1[1]+arr1[1].substring(arr1[1].lastIndexOf(":"),arr1[1].length()));
		txt = keyInfo;
		keyInfo.set(valueInfo.toString().substring(0,valueInfo.toString().indexOf(":")));
		valueInfo.set(arr1[0]+valueInfo.toString().substring(valueInfo.toString().indexOf(":"),valueInfo.toString().length()));
		
		context.write(keyInfo,valueInfo);
		//Output = (key: word , Value: word:path:n:N)
		
		for (int i = 1; i < arr.length; i++) {
			keyInfo.set(arr1[0]);
			valueInfo.set(arr[i]+arr[i].substring(arr[i].lastIndexOf(":"),arr[i].length()));
			txt = keyInfo;
			keyInfo.set(valueInfo.toString().substring(0,valueInfo.toString().indexOf(":")));
			valueInfo.set(arr1[0]+valueInfo.toString().substring(valueInfo.toString().indexOf(":"),valueInfo.toString().length()));
			
			context.write(keyInfo,valueInfo);
			//Output = (key: word , Value: word:path:n:N)
		}
	}
}
	public static class InvertedIndexMapper2 extends 
	Mapper<Object,Text,Object,Text>{
	private Text keyInfo = new Text();
	private  Text valueInfo = new Text();
	
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		Text txt = new Text();
		String[] arr = value.toString().split(";");
		String[] arr1 = arr[0].split("\t");
		keyInfo.set(arr1[0]);
		String[] arr2 = arr1[1].split(":");
		String countOfWord = arr2[2];
		valueInfo.set(arr1[1]+":"+countOfWord);
		
		context.write(keyInfo,valueInfo);
		//Output = (key: word , Value: word:path:n:N)
		
		for (int i = 1; i < arr.length; i++) {
			keyInfo.set(arr1[0]);
			String[] arr3 = arr[i].split(":");
			valueInfo.set(arr[i]+":"+arr3[2]);
			
			context.write(keyInfo,valueInfo);
			//Output = (key: word , Value: word:path:n:N)
		}
	}
}
	public static class InvertedIndexMapper3 extends 
	Mapper<Object,Text,Object,Text>{
	private Text keyInfo = new Text();
	private  Text valueInfo = new Text();
	
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] arr = value.toString().split(";");
		String[] arr1 = arr[0].split("\t");
		keyInfo.set(arr1[0]);
		valueInfo.set(arr1[1]);
		
		context.write(keyInfo,valueInfo);
		
		for (int i = 1; i < arr.length; i++) {
			keyInfo.set(arr1[0]);
			valueInfo.set(arr[i]);
			
			context.write(keyInfo,valueInfo);
		}
	}
}
	public static class InvertedIndexCombiner 
	extends Reducer<Text, Text, Text, Text>{
	private Text info = new Text();
	
	protected void reduce(Text key, Iterable<Text> values,Context context)
			throws IOException, InterruptedException {
		
		int sum = 0;
		for(Text value : values){
			sum += Integer.parseInt(value.toString()); 
		}
		
		int splitIndex = key.toString().indexOf(">");
		info.set(key.toString().substring(splitIndex+1)+":"+sum);
		key.set(key.toString().substring(0,splitIndex));
		context.write(key, info);
		//Output = (key: word , Value: word:path:5)
	}	
}
	public static class InvertedIndexCombiner1 
	extends Reducer<Text, Text, Text, Text>{
	private Text info = new Text();
	
	protected void reduce(Text key, Iterable<Text> values,Context context)
			throws IOException, InterruptedException {
		
		int sum =0;
		ArrayList<String> temp = new ArrayList<String>();
		for(Text val : values) {
			temp.add(val.toString());
			sum += Integer.parseInt(val.toString().substring(val.toString().lastIndexOf(":")+1, val.toString().length()));
			
		}
		for(String s : temp) {
			info.set(s.substring(0,s.indexOf(":"))+":"+key.toString()+":"+s.substring(s.indexOf(":")+1,s.lastIndexOf(":"))+":"+Integer.toString(sum));
			String k1 = s.substring(0,s.indexOf(":"));
			if(!table.containsKey(k1)) {
				ArrayList<String> lst = new ArrayList<String>();
				lst.add(info.toString());
				table.put(k1, lst);
				context.write(new Text(s.substring(0,s.indexOf(":"))), info);
				//Output = (key: word , Value: word:path:5:25)

			}
			else {
				table.get(k1).add(info.toString());
			}			
		}
		
	}	
}
	
	public static class InvertedIndexCombiner2 
	extends Reducer<Text, Text, Text, Text>{
	private Text info = new Text();
	
	protected void reduce(Text key, Iterable<Text> values,Context context)
			throws IOException, InterruptedException {

		int sum =0;
		ArrayList<String> temp = new ArrayList<String>();
		for(Text val : values) {
			temp.add(val.toString());
			sum += Integer.parseInt(val.toString().substring(val.toString().lastIndexOf(":")+1, val.toString().length()));
			
		}
		for(String s : temp) {
			info.set(s.substring(0,s.lastIndexOf(":"))+":"+Integer.toString(sum));
			if(!table1.containsKey(key.toString())) {
				ArrayList<String> lst = new ArrayList<String>();
				lst.add(info.toString());
				table1.put(key.toString(), lst);
				context.write(new Text(s.substring(0,s.indexOf(":"))), info);
				//Output = (key: word , Value: word:path:n:N)
			}
			else {
				table1.get(key.toString()).add(info.toString());
			}			
		}
		

	}	
}
	public static class InvertedIndexCombiner3 
	extends Reducer<Text, Text, Text, Text>{
	private Text info = new Text();
	
	protected void reduce(Text key, Iterable<Text> values,Context context)
			throws IOException, InterruptedException {

		for(Text  val : values) {
			String[] arr = val.toString().split(":");
			double n = Double.parseDouble(arr[2]);
			double N = Double.parseDouble(arr[3]);
			double m = Double.parseDouble(arr[4]);
			double D = 18;
			double result = (n/N)*(Math.log(D/m));
			context.write(new Text(arr[0]), new Text(arr[0]+","+arr[1]+","+Double.toString(result)));	
		}
		

	}	
}
	
	public static class InvertedIndexReducer 
	extends Reducer<Text, Text, Text, Text>{
	
	private Text result = new Text();
	
	protected void reduce(Text key, Iterable<Text> values,Context context)
			throws IOException, InterruptedException {


		String fileList = new String();
		for(Text value : values){//value="0.txt:1"
			fileList += value.toString()+";";
		}
		result.set(fileList);
		context.write(key, result);
		//Output = (word>path,1);(word1>path1,1)...
	}
}
	public static class InvertedIndexReducer1 
	extends Reducer<Text, Text, Text, Text>{
	
	private Text result = new Text();
	
	protected void reduce(Text key, Iterable<Text> values,Context context)
			throws IOException, InterruptedException {

		if(table.get(key.toString())!=null) {
			String fileList = new String();
			for(String val : table.get(key.toString())) {
				fileList+= val+";";
			}
			context.write(key, new Text(fileList));
			//Output = (key: word , Value: word:path:5:25),(key: word , Value: word:path:8:12)...

		}
		else {
			context.write(key, new Text("NULL"));
		}
		
	}
}
	public static class InvertedIndexReducer2 
	extends Reducer<Text, Text, Text, Text>{
	
	private Text result = new Text();
	
	protected void reduce(Text key, Iterable<Text> values,Context context)
			throws IOException, InterruptedException {
		
		if(table1.get(key.toString())!=null) {
			String fileList = new String();
			for(String val : table1.get(key.toString())) {
				fileList+= val+";";
			}
			context.write(key, new Text(fileList));
			//Output = (key: word , Value: word:path:n:N)
		}
		
		
	}
}
  public static void main(String[] args) throws Exception {
	  Configuration conf = new Configuration();
	    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	    if (otherArgs.length <= 2) {
	      System.err.println("Usage: InvertedIndex <in> <out> <num_reduce>");
	      System.exit(2);
	    }
	    
	    
	    Job job = new Job(conf, "Job-1");
	    job.setJarByClass(InvertedIndex.class);
	    job.setMapperClass(InvertedIndexMapper.class);
	    job.setCombinerClass(InvertedIndexCombiner.class);
	    job.setReducerClass(InvertedIndexReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]+"Job1"));
	    job.setNumReduceTasks(Integer.parseInt(otherArgs[2]));	
	    job.waitForCompletion(true);
	    
	    Job job1 = new Job(conf, "Job-2");
	    job1.setJarByClass(InvertedIndex.class);
	    job1.setMapperClass(InvertedIndexMapper1.class);
	    job1.setCombinerClass(InvertedIndexCombiner1.class);
	    job1.setReducerClass(InvertedIndexReducer1.class);
	    job1.setOutputKeyClass(Text.class);
	    job1.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job1, new Path(otherArgs[1]+"Job1/part*"));
	    FileOutputFormat.setOutputPath(job1, new Path(otherArgs[1]+"Job2"));
	    job1.setNumReduceTasks(Integer.parseInt(otherArgs[2]));	
	    job1.waitForCompletion(true);
	    
	    
	    Job job2 = new Job(conf, "Job-3");
	    job2.setJarByClass(InvertedIndex.class);
	    job2.setMapperClass(InvertedIndexMapper2.class);
	    job2.setCombinerClass(InvertedIndexCombiner2.class);
	    job2.setReducerClass(InvertedIndexReducer2.class);
	    job2.setOutputKeyClass(Text.class);
	    job2.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job2, new Path(otherArgs[1]+"Job2/part*"));
	    FileOutputFormat.setOutputPath(job2, new Path(otherArgs[1]+"Job3"));
	    job2.setNumReduceTasks(Integer.parseInt(otherArgs[2]));	
	    job2.waitForCompletion(true);
	    
	    Job job3 = new Job(conf, "Job-4");
	    job3.setJarByClass(InvertedIndex.class);
	    job3.setMapperClass(InvertedIndexMapper3.class);
	    job3.setCombinerClass(InvertedIndexCombiner3.class);
	    job3.setReducerClass(InvertedIndexReducer.class);
	    job3.setOutputKeyClass(Text.class);
	    job3.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job3, new Path(otherArgs[1]+"Job3/part*"));
	    FileOutputFormat.setOutputPath(job3, new Path(otherArgs[1]+"Job4"));
	    job3.setNumReduceTasks(Integer.parseInt(otherArgs[2]));	
	    job3.waitForCompletion(true);

	    
  }
  
  public static void createStopWordList(String path) throws IOException {
	  
	  File file = new File(path); 
	  
	  BufferedReader br = new BufferedReader(new FileReader(file)); 
	  
	  String st; 
	  while ((st = br.readLine()) != null) 
	   stop_words.put(st, 1);
	  } 
  }
