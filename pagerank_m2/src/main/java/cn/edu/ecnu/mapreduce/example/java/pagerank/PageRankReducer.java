package cn.edu.ecnu.mapreduce.example.java.pagerank;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.*;
//import org.apache.hadoop.io.Text;

import javax.print.DocFlavor;
//import javax.xml.soap.Text;
import java.io.IOException;

public class PageRankReducer extends Reducer<Text, ReducePageRankWritable, Text, NullWritable> {

    private static final double D = 0.85;

    @Override
    protected void reduce(Text key, Iterable<ReducePageRankWritable> values, Context context)
        throws IOException, InterruptedException {
        String[] pageInfo = null;
        int totalPage = context.getConfiguration().getInt(PageRank.TOTAL_PAGE, 0);
        int iteration = context.getConfiguration().getInt(PageRank.ITERATION, 0);
        double sum = 0;

        for (ReducePageRankWritable value : values) {
            String tag = value.getTag();
//            System.out.println(tag);

            if (tag.equals(ReducePageRankWritable.PR_L)) {
                sum += Double.parseDouble(value.getData());
            }
            else if (tag.equals(ReducePageRankWritable.PAGE_INFO)) {
//                System.out.println(value.getData());
                pageInfo = value.getData().split(" ");
            }
        }
//        System.out.println(sum);

        double pageRank = (1-D) / totalPage + D * sum;
//        System.out.println(pageRank);

        pageInfo[1] = String.valueOf(pageRank);
        StringBuilder result = new StringBuilder();
        if (iteration == (PageRank.MAX_ITERATION - 1)) {
            result.append(pageInfo[0]).append(" ").append(String.format("%.5f", pageRank));
        }
        else {
            for (String data : pageInfo) {
                result.append(data).append(" ");
            }
        }

//        System.out.println(result.toString());
//        System.out.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
        context.write(new Text(result.toString()), NullWritable.get());
    }
}
