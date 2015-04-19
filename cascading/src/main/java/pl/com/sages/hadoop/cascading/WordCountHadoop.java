package pl.com.sages.hadoop.cascading;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowDef;
import cascading.flow.FlowProcess;
import cascading.flow.hadoop2.Hadoop2MR1FlowConnector;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.aggregator.Count;
import cascading.operation.regex.RegexSplitGenerator;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.Retain;
import cascading.property.AppProps;
import cascading.scheme.hadoop.TextDelimited;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

import java.io.Serializable;
import java.util.Properties;

/**
 * Cascading Hadoop Word Count example
 */
public class WordCountHadoop implements Serializable {

	public class ScrubFunction extends BaseOperation implements Function {

		public ScrubFunction(Fields fieldDeclaration) {
			super(1, fieldDeclaration);
		}

		public String scrubText(String text) {
			return text.trim().toLowerCase();
		}

		@Override
		public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
			TupleEntry args = functionCall.getArguments();
			String token = scrubText(args.getString(0));

			if (token.length() > 0) {
				Tuple result = new Tuple();
				result.add(token);
				functionCall.getOutputCollector().add(result);
			}
		}
	}

	public void run(String[] args) {
		Fields token = new Fields("token");
		Fields text = new Fields("text");

		Tap docTap = new Hfs(new TextLine(text), args[0]);
		Tap wcTap = new Hfs(new TextDelimited(true, "\t"), args[1]);

		RegexSplitGenerator splitter = new RegexSplitGenerator(token, "[ \\[\\]\\(\\),.]");

		Pipe docPipe = new Pipe("token");
		docPipe = new Each(docPipe, text, splitter, Fields.RESULTS);

		docPipe = new Each(docPipe, token, new ScrubFunction(token), Fields.RESULTS);

		Pipe wcPipe = new Pipe("wc", docPipe);
		wcPipe = new Retain(wcPipe, token);
		wcPipe = new GroupBy(wcPipe, token);
		wcPipe = new Every(wcPipe, Fields.ALL, new Count(), Fields.ALL);

		FlowDef flowDef = FlowDef.flowDef().setName("wc")
			.addSource(docPipe, docTap)
			.addTailSink(wcPipe, wcTap);

		//j.s.nowacki@gmail.com

		Properties properties = AppProps.appProps()
				.setJarClass(WordCountHadoop.class)
				.setName("word-count-cascading")
				.setVersion("1.2.3")
				.buildProperties();

		//properties.put("fs.defaultFS", "hdfs://sandbox:8020");
		//properties.put("hadoop.job.ugi", "hue");

		FlowConnector flowConnector = new Hadoop2MR1FlowConnector(properties);

		Flow flow = flowConnector.connect(flowDef);
		flow.complete();
	}

	public static void main(String[] args) {
		WordCountHadoop wc = new WordCountHadoop();
		wc.run(args);
	}
}
