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

    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
        this.rand = new Random();
    }

    public void nextTuple() {
        String toSay = sentences[rand.nextInt(sentences.length)];
        this.collector.emit(new Values(toSay)); // TODO: 2017/2/3 emit 用法
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("sentence"));
    }
}
