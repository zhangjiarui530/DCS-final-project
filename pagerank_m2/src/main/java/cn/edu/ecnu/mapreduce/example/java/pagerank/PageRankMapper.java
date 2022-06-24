package cn.edu.ecnu.mapreduce.example.java.pagerank;

import org.apache.hadoop.conf.Configured;
//import org.apache.hadoop.io.LongWritable;
//import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class PageRankMapper extends Mapper<LongWritable , Text, Text, ReducePageRankWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] pageInfo = value.toString().split(" ");
//        System.out.println(pageInfo);

        double pageRank = Double.parseDouble(pageInfo[1]);
//        System.out.println(pageRank);

        int outLink = (pageInfo.length-2)/2;
//        System.out.println(outLink);
//        System.out.println(".................................................");

        ReducePageRankWritable writable;
        writable = new ReducePageRankWritable();
        writable.setData(String.valueOf(pageRank / outLink));
        writable.setTag(ReducePageRankWritable.PR_L); //贡献值

        for (int i = 2; i < pageInfo.length; i += 2) {
//            System.out.println(pageInfo[i]);
            context.write(new Text(pageInfo[i]), writable);
        }
        writable = new ReducePageRankWritable();

//        System.out.println(value.toString());
        writable.setData(value.toString());
        writable.setTag(ReducePageRankWritable.PAGE_INFO); //网页信息
        context.write(new Text(pageInfo[0]), writable);
    }
}
