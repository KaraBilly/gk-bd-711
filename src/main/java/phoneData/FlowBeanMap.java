package phoneData;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class FlowBeanMap extends Mapper<LongWritable, Text,LongWritable, FlowBean> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException{
        String line = value.toString();
        List<String> res = Arrays.stream(line.split("\t")).filter(x->!x.isEmpty()).collect(Collectors.toList());
        FlowBean flowBean = new FlowBean(Integer.parseInt(res.get(8)),Integer.parseInt(res.get(7)));
        key.set(Long.parseLong(res.get(1)));
        context.write(key,flowBean);
    }
}
