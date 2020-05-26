package com.firenay.spark.demo;

import com.firenay.spark.utils.DelFile;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.Arrays;

/**
 * <p>Title: Words</p>
 * Description：E:\Java\Dev\Spark\spark\word.txt
 * date：2020/5/25 21:57
 */
public class Words {

	private static Logger logger = LoggerFactory.getLogger(Words.class);

	public static void main(String[] args) {

		DelFile.deleteDirectory(DelFile.FILEPATH);

		SparkConf conf = new SparkConf().setAppName("JavaWordCount").setMaster("local[4]");
		//创建sparkContext
		JavaSparkContext jsc = new JavaSparkContext(conf);

		JavaRDD<String> word = jsc.parallelize(Arrays.asList("is", "good", "hadoop", "is", "good", "nay", "spark", "lsl", "fireNay", "大数据1803"));

		//指定以后从哪里读取数据
		JavaRDD<String> lines = jsc.textFile(args[0]);
		inRead(lines);

		inWord(word);

		// 按照一定的比例去拆分
		inSplit(word);

		// 多集合合并
		union(jsc);

		// 多集合交集
		intersection(jsc);

		// 多集合差集
		subtract(jsc);

		// 笛卡尔集运算
		cartesian(jsc);
		//释放资源
		jsc.stop();
	}

	/**
	 * 手动输入字符串的方式
	 */
	private static void inWord(JavaRDD<String> word){
		logger.warn("\n===================手动输入字符串的方式===================");

		JavaPairRDD<String, Integer> pairRDD = word.mapToPair(w -> new Tuple2<>(w, 1));
		// 对所有的字符串进行统计
		JavaPairRDD<String, Integer> byKey = pairRDD.reduceByKey((n, m) -> n + m);
		byKey.collect().forEach(System.out::println);
	}

	/**
	 * 从文件读取字符串的方式
	 */
	private static void inRead(JavaRDD<String> lines){
		logger.warn("\n===================从文件读取字符串的方式===================");
		//切分压平
		JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
		for (String word : words.collect()) {
			System.out.println(word);
		}
		words.distinct();
		String reduce = words.reduce((word1, word2) -> word1 + "^_^\t" +  word2);
		System.out.println(reduce);
	}

	/**
	 * 拆分集合
	 */
	private static void inSplit(JavaRDD<String> word){
		logger.warn("\n===================拆分集合===================");
		double[] scale = {0.2, 0.2, 0.6};
		JavaRDD<String>[] randomWord = word.randomSplit(scale);
		for (int i = 0; i < randomWord.length; i++) {
			System.out.println(randomWord[i].collect());
		}
	}

	/**
	 * 多集合合并
	 */
	private static void union(JavaSparkContext jsc){
		logger.warn("\n===================多集合合并===================");
		JavaRDD<Integer> intRDD1 = jsc.parallelize(Arrays.asList(10, 20, 30, 40, 50));
		JavaRDD<Integer> intRDD2 = jsc.parallelize(Arrays.asList(60, 100, 90));
		JavaRDD<Integer> intRDD3 = jsc.parallelize(Arrays.asList(110, 70, 120, 80));
		System.out.println(intRDD1.union(intRDD2).union(intRDD3).collect());
	}

	/**
	 * 多集合合并
	 */
	private static void intersection(JavaSparkContext jsc){
		logger.warn("\n===================多集合交集===================");
		JavaRDD<Integer> intRDD1 = jsc.parallelize(Arrays.asList(10, 20, 30, 60, 50));
		JavaRDD<Integer> intRDD2 = jsc.parallelize(Arrays.asList(20, 30, 90));
		JavaRDD<Integer> intRDD3 = jsc.parallelize(Arrays.asList(30, 60, 20, 90));
		System.out.println(intRDD1.intersection(intRDD2).intersection(intRDD3).collect());
	}

	/**
	 * 多集合差集：intRDD2、intRDD3在intRDD1中都没有的元素称之为差集
	 */
	private static void subtract(JavaSparkContext jsc){
		logger.warn("\n===================多集合差集===================");
		JavaRDD<Integer> intRDD1 = jsc.parallelize(Arrays.asList(20, 10, 30, 60, 50));
		JavaRDD<Integer> intRDD2 = jsc.parallelize(Arrays.asList(20, 30, 90));
		JavaRDD<Integer> intRDD3 = jsc.parallelize(Arrays.asList(30, 90, 20, 60));
		System.out.println(intRDD1.subtract(intRDD2).subtract(intRDD3).collect());
	}

	/**
	 * 笛卡尔集运算
	 */
	private static void cartesian(JavaSparkContext jsc){
		logger.warn("\n===================笛卡尔集运算===================");
		JavaRDD<Integer> intRDD1 = jsc.parallelize(Arrays.asList(20, 10, 30, 60, 50));
		JavaRDD<Integer> intRDD2 = jsc.parallelize(Arrays.asList(20, 30, 90));
		System.out.println(intRDD1.cartesian(intRDD2).collect());
	}
}
