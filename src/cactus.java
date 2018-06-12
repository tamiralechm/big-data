import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class cactus {

public static class cactusMapper extends Mapper<Object, Text, Text, Text>{

	//split input text and select out the column values "solver" "real"
public void map(Object key, Text values, Context context) throws IOException, InterruptedException{
String solverRecord = values.toString();
String[] solverDetailsLine = solverRecord.split("\t");
String solverId = "";
String real = "";
for(int i=1; i<solverDetailsLine.length; i++) {

	//to remove the header "real"
if(solverDetailsLine[14].contains("solved")&& !solverDetailsLine[11].contains("Real"))
{
solverId = solverDetailsLine[0];
real = solverDetailsLine[11];

}
}
context.write(new Text(solverId), new Text(real));
 
}
}

public static class SolverReduce extends Reducer<Text, Text, Text, Text>{
private Text runTime = new Text();
public void reduce(Text key,Iterable<Text> values, Context context) throws IOException, InterruptedException{
String time ="";
for(Text real: values) {
if(time.length()>0)
time+=",";
time+=real.toString();
}
runTime.set(time);
context.write(key, runTime);

}
}

 
public static void main(String[] args) throws Exception {
Configuration conf = new Configuration();
Job job = Job.getInstance(conf, "Solver Comparizion");
job.setJarByClass(cactus.class);
job.setMapperClass(cactusMapper.class);
job.setCombinerClass(SolverReduce.class);
job.setReducerClass(SolverReduce.class);
job.setOutputKeyClass(Text.class);
job.setOutputValueClass(Text.class);
FileInputFormat.addInputPath(job, new Path(args[0]));
FileOutputFormat.setOutputPath(job, new Path(args[1]));
System.exit(job.waitForCompletion(true) ? 0 : 1);
}
}
