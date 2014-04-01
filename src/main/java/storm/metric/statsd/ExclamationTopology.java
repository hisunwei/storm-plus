package storm.metric.statsd;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.metric.api.CountMetric;
import backtype.storm.metric.api.MeanReducer;
import backtype.storm.metric.api.MultiCountMetric;
import backtype.storm.metric.api.ReducedMetric;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.testing.TestWordSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

/**
 * This is a basic example of a Storm topology.
 */
public class ExclamationTopology {

	public static class ExclamationBolt extends BaseRichBolt {
		OutputCollector _collector;

		// Metrics
		// Note: these must be declared as transient since they are not
		// Serializable
		transient CountMetric _countMetric;
		transient MultiCountMetric _wordCountMetric;
		transient ReducedMetric _wordLengthMeanMetric;

		@Override
		public void prepare(Map conf, TopologyContext context,
				OutputCollector collector) {
			_collector = collector;

			// Metrics must be initialized and registered in the prepare()
			// method for bolts,
			// or the open() method for spouts. Otherwise, an Exception will be
			// thrown
			initMetrics(context);
		}

		void initMetrics(TopologyContext context) {
			_countMetric = new CountMetric();
			_wordCountMetric = new MultiCountMetric();
			_wordLengthMeanMetric = new ReducedMetric(new MeanReducer());

			context.registerMetric("execute_count", _countMetric, 5);
			context.registerMetric("word_count", _wordCountMetric, 60);
			context.registerMetric("word_length", _wordLengthMeanMetric, 60);
		}

		@Override
		public void execute(Tuple tuple) {
			_collector.emit(tuple, new Values(tuple.getString(0) + "!!!"));
			_collector.ack(tuple);

			updateMetrics(tuple.getString(0));
		}

		void updateMetrics(String word) {
			_countMetric.incr();
			_wordCountMetric.scope(word).incr();
			_wordLengthMeanMetric.update(word.length());
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("word"));
		}

	}

	public static void main(String[] args) throws Exception {
		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("word", new TestWordSpout(), 10);
		builder.setBolt("exclaim1", new ExclamationBolt(), 3).shuffleGrouping(
				"word");
		builder.setBolt("exclaim2", new ExclamationBolt(), 2).shuffleGrouping(
				"exclaim1");

		Config conf = new Config();
		conf.setDebug(true);

		// This will simply log all Metrics received into
		// $STORM_HOME/logs/metrics.log on one or more worker nodes.
//		conf.registerMetricsConsumer(LoggingMetricsConsumer.class, 2);
		Map reg = new HashMap();
		reg.put("metrics.statsd.host", "localhost");
		
		conf.registerMetricsConsumer(StatsdMetricConsumer.class,reg, 2);

		if (args != null && args.length > 0) {
			conf.setNumWorkers(3);

			StormSubmitter.submitTopology(args[0], conf,
					builder.createTopology());
		} else {

			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("test", conf, builder.createTopology());
			Utils.sleep(5 * 60 * 1000L);
			cluster.killTopology("test");
			cluster.shutdown();
		}
	}
}