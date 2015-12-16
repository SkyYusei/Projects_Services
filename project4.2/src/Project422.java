import org.apache.spark.Accumulator;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import scala.Tuple2;
import com.google.common.base.Optional;

import java.io.Serializable;
import java.util.*;

public class Project422 {
    public static void main(String[] args) {

        JavaSparkContext spark = new JavaSparkContext();
        JavaRDD<String> file = spark.textFile("s3://f15-p42/twitter-graph.txt");

        JavaPairRDD<String,String> idpair = file.mapToPair(new PairFunction<String, String, String>() {
			private static final long serialVersionUID = 1L;

			@Override
            public Tuple2<String, String> call(String line) throws Exception {
                String[] linecomponents = line.split("\t");
                String id = linecomponents[0];
                String id2 = linecomponents[1];
                return new Tuple2<String, String>(id, id2);
            }
        });

        //all the instinct id
        JavaRDD<String> ids = file.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String s) throws Exception {
                return Arrays.asList(s.trim().split("\t"));
            }
        }).distinct().cache();

        final Long N = ids.count();

        //store the id and its score
        JavaPairRDD<String,Double> id_score = ids.mapToPair(new PairFunction<String, String, Double>() {
            @Override
            public Tuple2<String, Double> call(String s) throws Exception {
                return new Tuple2<String, Double>(s,1.0);
            }
        });

        //adjacentlist (1,[2,3,4])
        JavaPairRDD<String,Iterable<String>> adjacentlist = idpair.groupByKey().cache();

        //the adjacentlist with count and score
        JavaPairRDD<String, Tuple2<Optional<Iterable<String>>, Double>> adjacentlist_score;

        //contribution pair
        JavaRDD<Tuple2<String, Double>> id_contribution;

        //store the accumulated contribution
        JavaPairRDD<String, Double> id_score_add;


        for (int i=0;i<10;i++) {

            final Accumulator<Double> danglingScores = spark.accumulator(0.0);

            adjacentlist_score = adjacentlist.rightOuterJoin(id_score);

            id_contribution = adjacentlist_score.flatMap(new FlatMapFunction<Tuple2<String, Tuple2<Optional<Iterable<String>>, Double>>, Tuple2<String, Double>>() {
                @Override
                public Iterable<Tuple2<String, Double>> call(Tuple2<String, Tuple2<Optional<Iterable<String>>, Double>> s) throws Exception {


                    double score = s._2()._2();

                    ArrayList<Tuple2<String, Double>> id_contribute_ = new ArrayList<Tuple2<String, Double>>();
                    if (s._2()._1().isPresent()) {
                        int count = ((Collection<String>) s._2()._1().get()).size();
                        Iterator<String> is = ((Collection<String>) s._2()._1().get()).iterator();
                        while (is.hasNext()) {
                            id_contribute_.add(new Tuple2<String, Double>(is.next(), score / count));
                        }

                    } else {
                        danglingScores.add(score);
                    }
                    return id_contribute_;
                }
            });
            //in order to let the lazy function execute
            id_contribution.count();

            final double danglingTotal = danglingScores.value();

            id_score_add = JavaPairRDD.fromJavaRDD(id_contribution).reduceByKey(new Function2<Double, Double, Double>() {
                @Override
                public Double call(Double aDouble, Double aDouble2) throws Exception {
                    return aDouble + aDouble2;
                }
            });


            id_score = id_score_add.mapToPair(new PairFunction<Tuple2<String, Double>, String, Double>() {
                @Override
                public Tuple2<String, Double> call(Tuple2<String, Double> s) throws Exception {
                    double add = s._2();
                    double newScore = 0.15 + 0.85 * (add + danglingTotal / N);
                    return new Tuple2<String, Double>(s._1(), newScore);
                }
            });
        }

        JavaRDD<String> file2 = spark.textFile("s3n://s15-p42-part2/wikipedia_mapping");
        //JavaRDD<String> file2 = spark.textFile("s3n://pmproject42/micro3");
        JavaPairRDD<String,String> namepair = file2.mapToPair(new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String line) throws Exception {
                String[] linecomponents = line.split("\t");
                String id = linecomponents[0];
                String name = linecomponents[1];
                return new Tuple2<String, String>(id, name);
            }
        });

        //sort
        class MyComparator implements  Comparator<String>,Serializable {
            @Override
            public int compare(String o1, String o2) {
                String title1,title2;
                title1=o1.split("\t")[0];
                title2=o2.split("\t")[0];
                double pr1,pr2;
                pr1=Double.valueOf(o1.split("\t")[1]);
                pr2=Double.valueOf(o2.split("\t")[1]);
                if(pr1-pr2<0){
                    return 1;
                }else if(pr1-pr2>0){
                    return -1;
                }else{
                    return title1.compareTo(title2);
                }
            }
        }

        JavaRDD<String> output = namepair.join(id_score).map(new Function<Tuple2<String, Tuple2<String, Double>>, String>() {
            @Override
            public String call(Tuple2<String, Tuple2<String, Double>> s) throws Exception {
                return s._2()._1() + "\t" + String.valueOf(s._2()._2());
            }
        });

        List<String> top100 = output.takeOrdered(100, new MyComparator());

        JavaRDD<String> top100rdd = spark.parallelize(top100);


        top100rdd.saveAsTextFile("s3n://pmproject42/output2");

        spark.close();
    }
}