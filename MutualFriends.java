import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.Period;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class MutualFriends extends Configured implements Tool {

    public static class FirstMap extends Mapper<LongWritable,Text,Text,Text>
    {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] str=value.toString().split("\\t");
            String str_key=str[0];
            if(str.length==2)
            {
                int to_compare = Integer.parseInt(str_key);
                StringTokenizer stringTokenizer = new StringTokenizer(str[1], "\\,");
                Text intermediate_key = new Text();
                Text intermediate_value = new Text();
                intermediate_value.set(str[1]);
                while (stringTokenizer.hasMoreTokens()) {
                    String friend = stringTokenizer.nextToken().trim();
                    int friend_compare = Integer.parseInt(friend);
                    if (to_compare < friend_compare)
                        intermediate_key.set(str_key + "," + friend);
                    else
                        intermediate_key.set(friend + "," + str_key);
                    context.write(intermediate_key, intermediate_value);
                }
            }
        }
    }

    public static class FirstReduce extends Reducer<Text,Text,Text,Text>
    {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Iterator<Text> iterator=values.iterator();
            ArrayList<String[]> arrayList=new ArrayList<String[]>();
            boolean flag=false;
            while(iterator.hasNext())
            {
                String[] s=iterator.next().toString().split(",");
                arrayList.add(s);
            }
            String final_string = "";
            HashSet<String> mutual_friends = new HashSet<String>();
            for(String[] friends: arrayList){
                Arrays.sort(friends);
                for(String friend: friends){
                    if(mutual_friends.contains(friend))
                    {
                        final_string += friend + ",";
                        flag=true;
                    }
                    else
                        mutual_friends.add(friend);
                }
            }
            if(flag) {
                Text final_value = new Text();
                final_string=final_string.substring(0,final_string.length()-1);
                final_value.set(final_string);
                context.write(key, final_value);
            }
        }
    }
    public static class SecondMap extends Mapper<LongWritable,Text,Text,Text>{
        public void map(LongWritable key,Text value, Context context) throws IOException, InterruptedException {
            String[] strings=value.toString().split("\\t");
            String pair=strings[0];
            String mutual=strings[1];
            String[] list_of_friends=mutual.split(",");
            int count=list_of_friends.length;
            Text intermediate_value=new Text();
            intermediate_value.set(pair+"\t"+mutual+"\t\t"+count);
            Text intermediate_key=new Text();
            intermediate_key.set("");
            context.write(intermediate_key,intermediate_value);
        }
    }
    public static class SecondReduce extends Reducer<Text,Text,Text,Text>{
        int count;
        public void reduce (Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException {
            Iterator<Text> iterator=values.iterator();
            Map<Integer,List<String>> treeMap=new TreeMap<>(Collections.reverseOrder());
            while(iterator.hasNext()){
                String[] to_write=iterator.next().toString().split("\t\t");
                count=Integer.parseInt(to_write[1].trim());
                List<String> count_list=new ArrayList<>();
                count_list.add(to_write[0]);
                if(treeMap.containsKey(count))
                {
                    List<String> temp=treeMap.get(count);
                    count_list.addAll(temp);
                    treeMap.put(count,count_list);
                }
                else
                {
                    treeMap.put(count,count_list);
                }
            }
            int max_list=0;
            boolean flag=false;
            for (Map.Entry<Integer, List<String>> entry : treeMap.entrySet()) {
                count = entry.getKey();
                List<String> map_values = entry.getValue();
                for(String ss:map_values)
                {
                    String key_string=ss.split("\t")[0].trim();
                    String list=ss.split("\t")[1].trim();
                    Text key_to_write=new Text();
                    key_to_write.set(key_string);
                    Text value_to=new Text();
                    value_to.set(count+"\t"+list);
                    context.write(key_to_write,value_to);
                    max_list++;
                    if(max_list==10)
                    {
                        flag=true;
                        break;
                    }
                }
                if(flag)
                    break;
            }
        }
    }

    public static class ThirdMap extends Mapper<LongWritable,Text,Text,Text>{
        BufferedReader br;
        HashMap<String,String> hashMap=new HashMap<>();
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration configuration=context.getConfiguration();
            br= new BufferedReader(new InputStreamReader(FileSystem.get(configuration).open(new Path(configuration.get("input_path")))));
            String s;
            String[] fields;
            while ((s=br.readLine())!=null)
            {
                fields=s.split(",");
                hashMap.put(fields[0],fields[1]+": "+fields[4]);
            }
        }

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] strings=value.toString().split("\\t");
            String pair=strings[0];
            String mutual=strings[1];
            int i;
            String value_to_write="[";
            String[] list_of_friends=mutual.split(",");
            for(String s:list_of_friends)
            {
                value_to_write=value_to_write+hashMap.get(s)+", ";
            }
            Text intermediate_value=new Text();
            value_to_write=value_to_write.substring(0,value_to_write.length()-2);
            value_to_write=value_to_write+"]";
            intermediate_value.set(value_to_write);
            Text intermediate_key=new Text();
            intermediate_key.set(pair);
            context.write(intermediate_key,intermediate_value);
        }
    }
    public static class FourthMap_socialdata extends Mapper<LongWritable,Text,Text,Text>{
        BufferedReader br;
        HashMap<String,String> hashMap=new HashMap<>();
        SimpleDateFormat sdf=new SimpleDateFormat("MM/dd/yyyy");
        Date d;
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration configuration=context.getConfiguration();
            br= new BufferedReader(new InputStreamReader(FileSystem.get(configuration).open(new Path(configuration.get("input_path")))));
            String s;
            String[] fields;
            try
            {
                while ((s=br.readLine())!=null)
                {
                    fields=s.split(",");
                    String s1=fields[9];
                    d = sdf.parse(s1);
                    Calendar c=Calendar.getInstance();
                    c.setTime(d);
                    int year=c.get(Calendar.YEAR);
                    int month=c.get(Calendar.MONTH)+1;
                    int date=c.get(Calendar.DATE);
                    LocalDate l1 = LocalDate.of(year, month, date);
                    LocalDate now1 = LocalDate.now();
                    Period diff1 = Period.between(l1, now1);
                    hashMap.put(fields[0],diff1.getYears()+"");
                }
                br.close();
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] strings=value.toString().split("\\t");
            String pair=strings[0];
            String value_to_write="social\t";
            if(strings.length==2)
            {
                String[] mutual=strings[1].split(",");
                for(String s:mutual)
                {
                    value_to_write=value_to_write+hashMap.get(s)+",";
                }
                Text intermediate_value=new Text();
                intermediate_value.set(value_to_write);
                Text intermediate_key=new Text();
                intermediate_key.set(pair);
                context.write(intermediate_key,intermediate_value);
            }
        }
    }

    public static class FourthMap_userdata extends Mapper<LongWritable,Text,Text,Text>{

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] strings=value.toString().split(",");
            String pair=strings[0];
            String mutual="user\t"+strings[3]+","+strings[4]+","+strings[5];
            Text intermediate_value=new Text();
            intermediate_value.set(mutual);
            Text intermediate_key=new Text();
            intermediate_key.set(pair);
            context.write(intermediate_key,intermediate_value);
        }
    }
    public static class FourthReduce extends Reducer<Text,Text,Text,Text>{

        String address;
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int count=0;
            int age=0;
            for(Text t:values)
            {
                String[] s=t.toString().split("\t");
                if(s[0].equals("social"))
                {
                    String[] ages=s[1].split(",");
                    for(String a:ages)
                    {
                        age+=Integer.parseInt(a);
                        count++;
                    }
                    age=age/count;

                }
                if(s[0].equals("user"))
                {
                    address=s[1];
                }
            }
            if(age!=0)
                context.write(key,new Text(address+"\t\t"+age));
        }
    }
    public static class FourthMap_top15 extends Mapper<LongWritable,Text,Text,Text>{
        public void map(LongWritable key,Text value, Context context) throws IOException, InterruptedException {
            Text intermediate_value=new Text();
            intermediate_value.set(value.toString());
            Text intermediate_key=new Text();
            intermediate_key.set("");
            context.write(intermediate_key,intermediate_value);
        }
    }
    public static class FourthReduce_top15 extends Reducer<Text,Text,Text,Text>{
        public void reduce (Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException {
            Iterator<Text> iterator=values.iterator();
            Map<Integer,List<String>> treeMap=new TreeMap<>();
            while(iterator.hasNext())
            {
                String[] to_write=iterator.next().toString().split("\t\t");
                int age=Integer.parseInt(to_write[1].trim());
                List<String> count_list=new ArrayList<>();
                count_list.add(to_write[0]);
                if(treeMap.containsKey(age))
                {
                    List<String> temp=treeMap.get(age);
                    count_list.addAll(temp);
                    treeMap.put(age,count_list);
                }
                else
                {
                    treeMap.put(age,count_list);
                }
            }
            int max_list=0;
            boolean flag=false;
            for (Map.Entry<Integer, List<String>> entry : treeMap.entrySet()) {
                int age2 = entry.getKey();
                List<String> map_values = entry.getValue();
                for(String ss:map_values)
                {
                    String key_to=ss.split("\t")[0].trim();
                    String list=ss.split("\t")[1].trim();
                    Text key_to_write=new Text();
                    key_to_write.set(key_to);
                    Text value_to=new Text();
                    value_to.set(list+","+age2);
                    context.write(key_to_write,value_to);
                    max_list++;
                    if(max_list==15)
                    {
                        flag=true;
                        break;
                    }
                }
                if(flag)
                    break;
            }
        }
    }

    public static void main(String args[]) throws Exception {
        int result = ToolRunner.run(new MutualFriends(), args);
        System.exit(result);
    }

    @Override
    public int run(String[] args) throws Exception {

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        // get all args
        if (otherArgs.length != 3) {
            System.out.println("Enter valid number of arguments");
            System.out.println("<input1>\t<input2>\t<output>");
            System.exit(1);
        }
        Job job1=new Job(conf,"MutualFriends1");
        job1.setJarByClass(MutualFriends.class);
        job1.setMapperClass(FirstMap.class);
        job1.setReducerClass(FirstReduce.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job1,new Path(args[0]));
        FileOutputFormat.setOutputPath(job1,new Path(args[2]+"/q1"));
        job1.waitForCompletion(true);

        Job job2=new Job(conf,"MutualFriends2");
        job2.setJarByClass(MutualFriends.class);
        job2.setMapperClass(SecondMap.class);
        job2.setReducerClass(SecondReduce.class);
        job2.setNumReduceTasks(1);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job2,new Path(args[2]+"/q1"));
        FileOutputFormat.setOutputPath(job2,new Path(args[2]+"/q2"));
        job2.waitForCompletion(true);

        conf.set("input_path",args[1]);
        Job job3=new Job(conf,"MutualFriends3");
        job3.setJarByClass(MutualFriends.class);
        job3.setMapperClass(ThirdMap.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);
        job3.setNumReduceTasks(0);
        FileInputFormat.addInputPath(job3,new Path(args[2]+"/q1"));
        FileOutputFormat.setOutputPath(job3,new Path(args[2]+"/q3"));
        job3.waitForCompletion(true);

        Job job4=new Job(conf,"MutualFriends4");
        job4.setJarByClass(MutualFriends.class);
        job4.setReducerClass(FourthReduce.class);
        job4.setOutputKeyClass(Text.class);
        job4.setOutputValueClass(Text.class);
        MultipleInputs.addInputPath(job4,new Path(args[0]), TextInputFormat.class,FourthMap_socialdata.class);
        MultipleInputs.addInputPath(job4,new Path(args[1]), TextInputFormat.class,FourthMap_userdata.class);
        FileOutputFormat.setOutputPath(job4,new Path(args[2]+"/temp"));
        job4.waitForCompletion(true);

        Job job5=new Job(conf,"MutualFriends5");
        job5.setJarByClass(MutualFriends.class);
        job5.setMapperClass(FourthMap_top15.class);
        job5.setReducerClass(FourthReduce_top15.class);
        job5.setOutputKeyClass(Text.class);
        job5.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job5,new Path(args[2]+"/temp"));
        FileOutputFormat.setOutputPath(job5,new Path(args[2]+"/q4"));
        job5.waitForCompletion(true);
        FileSystem hdfs = FileSystem.get(conf);
        Path temp = new Path(args[2] + "/temp");
        if (hdfs.exists(temp))
        {
            hdfs.delete(temp, true);
        }

        return 0;
    }
}
