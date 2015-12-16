import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class Project42 {

    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf().setAppName("Word");
        //using m3.2xlarge
        sparkConf.set("spark.executor.memory", "40g");

        JavaSparkContext spark = new JavaSparkContext(sparkConf);
        JavaRDD<String> file = spark.textFile("s3n://s15-p42-part1-easy/data/");
        //JavaRDD<String> file = spark.textFile("s3n://pmproject42/micro");

        JavaRDD<String> titles = file.map(new Function<String, String>() {
            @Override
            public String call(String line) throws Exception {
                String[] linecomponents = line.split("\t");
                String id = linecomponents[0];
                String title = linecomponents[1];
                return title;
            }
        });

        final long N = titles.count();

        // return (word,title)
        JavaRDD<Tuple2<String,String>> word_titles = file.flatMap(new FlatMapFunction<String, Tuple2<String, String>>() {
            @Override
            public Iterable<Tuple2<String, String>> call(String line) throws Exception {
                line = line.trim();
                String[] linecomponents = line.split("\t");
                String id = linecomponents[0];
                String title = linecomponents[1];
                String content = linecomponents[3];
                content = content.replaceAll("<[^>]*>", " ").replace("\\n", "\n").replaceAll("[^a-zA-Z]+", " ").trim().toLowerCase();
                String[] words = content.split(" ");

                ArrayList<Tuple2<String, String>> word_title = new ArrayList<Tuple2<String, String>>();
                for (int i = 0; i < words.length; i++) {
                    word_title.add(new Tuple2<String, String>(words[i], title));
                }
                return word_title;
            }
        }).persist(StorageLevel.MEMORY_AND_DISK());

        // key: word value 1, count distinct title for each word
        JavaPairRDD<String,Long> word_1 = word_titles.distinct().mapToPair(new PairFunction<Tuple2<String, String>, String, Long>() {
            @Override
            public Tuple2<String, Long> call(Tuple2<String, String> s) throws Exception {
                String word = s._1();
                return new Tuple2<String, Long>(word, 1l);
            }
        });

        //count key: word value:count
        JavaPairRDD<String, Long> word_total = word_1.reduceByKey(new Function2<Long, Long, Long>() {
            @Override
            public Long call(Long a, Long b) { return a + b; }
        });


        // key: (word,title), value:1
        JavaPairRDD<Tuple2<String,String>, Long> word_title_1 = word_titles.mapToPair(new PairFunction<Tuple2<String, String>, Tuple2<String, String>, Long>() {
            @Override
            public Tuple2<Tuple2<String, String>, Long> call(Tuple2<String, String> s) throws Exception {
                return new Tuple2<Tuple2<String, String>, Long>(s, 1l);
            }
        });

        word_titles.unpersist();

        //count key:word,title value:count
        JavaPairRDD<Tuple2<String,String>, Long> word_title_total = word_title_1.reduceByKey(new Function2<Long, Long, Long>() {
            @Override
            public Long call(Long a, Long b) { return a + b; }
        });

        //transform to key:word  value:title,count
        JavaPairRDD<String,Tuple2<String,Long>> word_title_total2 = word_title_total.mapToPair(new PairFunction<Tuple2<Tuple2<String, String>, Long>, String, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Tuple2<String, Long>> call(Tuple2<Tuple2<String, String>, Long> s) throws Exception {
                String word = s._1()._1();
                String title = s._1()._2();
                Long count = s._2();
                return new Tuple2<String, Tuple2<String, Long>>(word,new Tuple2<String, Long>(title,count));
            }
        });

        //join (word,(title,word_in_title_count)) with (word,title_with_word_count)
        JavaPairRDD<String,Tuple2<Tuple2<String,Long>,Long>> word_title_total_dtotal = word_title_total2.join(word_total);

        //get only cloud
        JavaPairRDD<String,Tuple2<Tuple2<String,Long>,Long>> word_title_total_dtotal_cloud = word_title_total_dtotal.filter(new Function<Tuple2<String, Tuple2<Tuple2<String, Long>, Long>>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, Tuple2<Tuple2<String, Long>, Long>> s) throws Exception {
                String word = s._1();
                return word.equals("cloud");
            }
        });

        //calculte tf_idf output title\tf_idf
        final JavaRDD<String> tf_idf = word_title_total_dtotal_cloud.map(new Function<Tuple2<String, Tuple2<Tuple2<String, Long>, Long>>, String>() {
            @Override
            public String call(Tuple2<String, Tuple2<Tuple2<String, Long>, Long>> s) throws Exception {
                String word = s._1();
                String title = s._2()._1()._1();
                Long wcount = s._2()._1()._2();
                Long dcount = s._2()._2();
                double idf = (double) N / dcount;
                double tfidf = wcount * Math.log10(idf);
                //return word+"\t"+title + "\t" + String.valueOf(tfidf);
                return title + "\t" + String.valueOf(tfidf);
            }
        });

        //sort
        class MyComparator implements  Comparator<String>,Serializable{
            @Override
            public int compare(String o1, String o2) {
                String title1,title2;
                title1=o1.split("\t")[0];
                title2=o2.split("\t")[0];
                double tf_idf1,tf_idf2;
                tf_idf1=Double.valueOf(o1.split("\t")[1]);
                tf_idf2=Double.valueOf(o2.split("\t")[1]);
                if(tf_idf1-tf_idf2<0){
                    return 1;
                }else if(tf_idf1-tf_idf2>0){
                    return -1;
                }else{
                    return title1.compareTo(title2);
                }
            }
        }

        List<String> top100 = tf_idf.takeOrdered(100, new MyComparator());

        JavaRDD<String> top100rdd = spark.parallelize(top100);
        top100rdd.saveAsTextFile("s3n://pmproject42/output");

        spark.close();

    }
}