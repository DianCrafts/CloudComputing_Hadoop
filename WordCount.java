import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Comparator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import java.nio.ByteBuffer;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.regex.*;  


import static java.lang.Math.*;
public class WordCount {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, Text>{
    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
String  str= value.toString().replaceAll("(\"[^\"]*\")", "").split(",")[12];

 if(Pattern.matches("[0-9]{1,13}(\\.0)", str)){
    Double district = Double.parseDouble(str);
  if (!str.equals("District")) {
        word.set(Integer.toString((int) Math.round(district)));//district.toString()
        int x = 1;
      
             context.write(word,new Text(Integer.toString(x)));
       }
     
    }
 }
  }

  public static class IntSumReducer
       extends Reducer<Text,Text,Text,Text> {
   // private LongWritable result = new LongWritable();

    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (Text val : values) {
        int x = (int) Math.round(Double.parseDouble(val.toString()));
        sum += Integer.parseInt(val.toString());
      }
      context.write(key,new Text(Integer.toString(sum)));
    }
  }






  public static void commandRunner(String str){
     ProcessBuilder processBuilder = new ProcessBuilder();

        // -- Linux --

        // Run a shell command
        processBuilder.command("bash", "-c", str);

        try {

            Process process = processBuilder.start();

            StringBuilder output = new StringBuilder();

            BufferedReader reader = new BufferedReader(
                    new InputStreamReader(process.getInputStream()));

            String line;
            while ((line = reader.readLine()) != null) {
                output.append(line + "\n");
            }

            int exitVal = process.waitFor();
            if (exitVal == 0) {
                System.out.println("Success!");
                System.out.println(output);
             
            } else {
                //abnormal...
            }

        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
  }

 public static void main(String[] args) throws Exception {
      Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(WordCount.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);

    job.setReducerClass(IntSumReducer.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
     if (!job.waitForCompletion(true)) {
      System.exit(1);
    }

    String command = "hadoop fs -cat /user/Hadoop/WordCount/output/part-r-00000 > out.txt";

commandRunner(command);

commandRunner("sort -k2 -g -r out.txt >out2.txt");

commandRunner("sed 's/ \\+/,/g' out2.txt > out2.csv ");



 }

}
