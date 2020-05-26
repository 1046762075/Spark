package com.firenay.spark;

import com.firenay.spark.utils.DelFile;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.ListIterator;

/**
 * <p>Title: JavaLambdaWordCount</p>
 * Description：E:\Java\Dev\Spark\spark\word.txt E:\Java\Dev\Spark\spark\out\
 * date：2020/5/25 20:39
 */
public class JavaLambdaWordCount {

    public static void main(String[] args) {

		DelFile.deleteDirectory(DelFile.FILEPATH);

        SparkConf conf = new SparkConf().setAppName("JavaWordCount").setMaster("local[4]");
        //创建sparkContext
        JavaSparkContext jsc = new JavaSparkContext(conf);
        //指定以后从哪里读取数据
        JavaRDD<String> lines = jsc.textFile(args[0]);
        //切分压平
        JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
        //将单词和一组合
        JavaPairRDD<String, Integer> wordAndOne = words.mapToPair(w -> new Tuple2<>(w, 1));
        //聚合
        JavaPairRDD<String, Integer> reduced = wordAndOne.reduceByKey((m, n) -> m + n);
        //调整顺序
        JavaPairRDD<Integer, String> swaped = reduced.mapToPair(tp -> tp.swap());
        //排序
        JavaPairRDD<Integer, String> sorted = swaped.sortByKey(false);
        //调整顺序
        JavaPairRDD<String, Integer> result = sorted.mapToPair(tp -> tp.swap());
		// 将内容输出到控制台
		ListIterator<Tuple2<String, Integer>> iterator = result.collect().listIterator();
		while(iterator.hasNext()){
			System.out.println(iterator.next());
		}
		//将结果保存到hdfs
		result.saveAsTextFile(args[1]);
        //释放资源
        jsc.stop();
    }
}
