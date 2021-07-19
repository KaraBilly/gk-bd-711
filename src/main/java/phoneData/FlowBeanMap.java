package phoneData;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class FlowBeanMap extends Mapper<String, Text,String, IntWritable> {

    public void mapper(String key,Text value,Context context) throws IOException,InterruptedException{
        List<String> res = Arrays.stream(key.split("\t")).filter(x->!x.isEmpty()).collect(Collectors.toList());
        FlowBean flowBean = new FlowBean(Integer.parseInt(res.get(8)),Integer.parseInt(res.get(7)));
        context.write(res.get(1),flowBean);
    }
}
