package streaming;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaMapWithStateDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import redis.RedisClientPool;
import redis.clients.jedis.Jedis;
import scala.Tuple2;

import java.util.*;
import java.util.regex.Pattern;

/**
 * Created by lenovo on 2016/12/5.
 */
public class JavaKafkaActive_11201 {

    private static final Logger logger = Logger.getLogger(JavaKafkaActive_11201.class);

    private static final Pattern SPACE = Pattern.compile("\\|");

    private static final String Redis_Hss = "hss_data";

    private static final String Redis_Itc = "news_11201_";

    private static final String Redis_Tot = "tot_act_";

    public static void main(String[] args)  {
        if (args.length < 4) {
            System.err.println("Usage: JavaKafkaActive_11201 <zkQuorum> <group> <topics> <numThreads>");
            System.exit(1);
        }

        SparkConf sparkConf = new SparkConf().setAppName("JavaKafkaActive_11201");
        // Create the context with 2 seconds batch size
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(2000));

        jssc.checkpoint("/home/hadoop/spark_check");
        int numThreads = Integer.parseInt(args[3]);
        Map<String, Integer> topicMap = new HashMap<String,Integer>();
        String[] topics = args[2].split(",");
        for (String topic: topics) {
            topicMap.put(topic, numThreads);
        }

        JavaPairReceiverInputDStream<String, String> messages =
                KafkaUtils.createStream(jssc, args[0], args[1], topicMap);

        JavaDStream<String> lines = messages.map(new Function<Tuple2<String, String>, String>() {
            @Override
            public String call(Tuple2<String, String> tuple2) {
                return tuple2._2();
            }
        });

        JavaMapWithStateDStream<Tuple2<String, String>, Integer, Integer, Tuple2<Tuple2<String ,String>, Integer>> activeStream =
                lines.flatMapToPair(new PairFlatMapFunction<String, String, String>() {
                    @Override
                    public Iterator<Tuple2<String, String>> call(String x) throws Exception {
                        List<Tuple2<String, String>> list = new ArrayList<Tuple2<String, String>>();
                        String form_phone = null;
                        String to_phone = null;
                        String day = null;
                        try{
                            String[] words = SPACE.split(x);

                            if (words.length > 3) {
                                day = words[0].substring(0, 8);
                                if (words[2].length() > 0) {
                                    form_phone = words[2].substring(3, 14);
                                }
                                if (words[3].length() > 0) {
                                    to_phone = words[3].substring(3, 14);
                                }
                            }
                        }catch(Exception e){
                            logger.warn(e.toString());
                        }
                        if(form_phone != null) {
                            list.add(new Tuple2<String, String>(form_phone, day));
                        }

                        if(to_phone != null) {
                            list.add(new Tuple2<String, String>(to_phone, day));
                        }

                        return list.iterator();
                    }
                }).filter(new Function<Tuple2<String, String>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<String, String> v1) throws Exception {
                        String phone = v1._1();
                        String day = v1._2();
                        Jedis jedis = null;
                        try {
                             jedis = RedisClientPool.getInstance().getJedis();
                            if (jedis.hexists(Redis_Hss, phone)) {
                                String reg_time = jedis.hget(Redis_Hss, phone);
                                int timeTest = day.compareTo(reg_time);
                                if (timeTest != -1) {
                                    return true;
                                }
                            }
                        }catch (Exception e){
                            logger.warn(e.toString());
                        }finally {
                            if(jedis != null)
                                 jedis.close();
                        }
                        return false;
                    }
                }).mapToPair(new PairFunction<Tuple2<String,String>, Tuple2<String ,String>, Integer>() {
                    @Override
                    public Tuple2<Tuple2<String, String>, Integer> call(Tuple2<String, String> stringStringTuple2) throws Exception {
                        String day = stringStringTuple2._2();
                        Tuple2<String,String> account = new Tuple2<String, String>(stringStringTuple2._1(),day);
                        return new Tuple2<Tuple2<String, String>, Integer>(account ,1);
                    }
                }).mapWithState(StateSpec.function(
                        new Function3<Tuple2<String, String>, Optional<Integer>, State<Integer>, Tuple2<Tuple2<String, String>, Integer>>() {
                            @Override
                            public Tuple2<Tuple2<String, String>, Integer> call(Tuple2<String, String> v1, Optional<Integer> one, State<Integer> state) throws Exception {
                                int old = state.exists() ? state.get() : 0;
                                if (old == 0) {
                                    Jedis jedis = null;
                                    try {
                                        jedis = RedisClientPool.getInstance().getJedis();
                                        jedis.pfadd(Redis_Itc + v1._2(), v1._1());
                                        jedis.pfadd(Redis_Tot + v1._2(), v1._1());
                                    }catch (Exception e){
                                        logger.warn(e.toString());
                                    }finally {
                                        if(jedis != null) {
                                            jedis.close();
                                        }
                                    }
                                }
                                int sum = one.or(0) + old;
                                Tuple2<Tuple2<String, String>, Integer> output = new Tuple2<Tuple2<String, String>, Integer>(v1, sum);
                                state.update(sum);
                                return output;
                            }
                        }));

        activeStream.print();
        jssc.start();
        try{
            jssc.awaitTermination();
        }catch(Exception e){
            logger.warn(e.toString());
        }
    }
}
