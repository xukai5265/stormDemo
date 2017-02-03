package cn.lingban.storm;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;
import java.util.Random;

/**
 * Created by xukai on 2016/12/26.
 */
public class RandomSpout extends BaseRichSpout{

    private SpoutOutputCollector collector;

    private Random rand;

    private static String[] sentences = new String[] {"edi:I'm happy", "marry:I'm ang"};

    /**
     * 初始化动作，允许你在该spout初始化时做一些动作，传入了上下文，方便取上下文的一些数据。
     * @param map
     * @param topologyContext
     * @param spoutOutputCollector
     */
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
        this.rand = new Random();
    }

    /**
     * 用来发送数据
     */
    public void nextTuple() {
        String toSay = sentences[rand.nextInt(sentences.length)];
        /*
            emit 作用：发射tuple
            我们不需要实现tuple，我们只需要定义tuple的value，Storm会帮助我们生成tuple
         */
        this.collector.emit(new Values(toSay));
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        /*
            outputFieldsDeclarer.declare方法用来给我们发射的value在整个Stream中定义一个别名。
            可以理解为key。该值必须在整个topology定义中唯一。
         */
        outputFieldsDeclarer.declare(new Fields("sentence"));
    }
}
