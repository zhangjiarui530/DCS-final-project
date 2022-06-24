package cn.edu.ecnu.mapreduce.example.java.pagerank;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.*;

public class PageRank extends Configured implements Tool {
    public static final int MAX_ITERATION = 20;
    private static int iteration = 0;
    public static final String TOTAL_PAGE = "1";
    public static final String ITERATION = "2";

    @Override
    public int run(String[] args) throws  Exception {
//        System.out.println(args[0]);
//        System.out.println(args[1]);
//        System.out.println(args[2]);
//        int totalPage = Integer.parseInt(args[2]);
        int totalPage = 10;

        getConf().setInt(PageRank.ITERATION, iteration);
        getConf().setInt(PageRank.TOTAL_PAGE, totalPage);

        Job job = Job.getInstance(getConf(), getClass().getSimpleName());
        job.setJarByClass(getClass());

        if (iteration == 0) {
            FileInputFormat.addInputPath(job, new Path(args[0]));
        }
        else {
            FileInputFormat.addInputPath(job, new Path(args[1] + (iteration - 1)));
        }

        FileOutputFormat.setOutputPath(job, new Path(args[1] + iteration));

        // 设置map方法及其输出键值对的数据类型
        job.setMapperClass(PageRankMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(ReducePageRankWritable.class);

        // 设置reduce方法及其输出键值对的数据类型
        job.setReducerClass(PageRankReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        return job.waitForCompletion(true) ? 0 : -1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = 0;

        long startTime = System.currentTimeMillis();
        while (iteration < MAX_ITERATION) {
            exitCode = ToolRunner.run(new PageRank(), args);

//            System.out.println(iteration);
//            System.out.println(exitCode);
//            System.out.println("====================================");

            if (exitCode == -1) {
                break;
            }
            iteration++;
        }
        long endTime = System.currentTimeMillis();
        System.out.println("程序运行时间：" + (endTime - startTime) + "ms");
    }
}
