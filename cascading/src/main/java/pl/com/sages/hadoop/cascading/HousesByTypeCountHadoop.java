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
public class HousesByTypeCountHadoop implements Serializable {

	public class ExtractColumns extends BaseOperation implements Function {

		public static final int TYPE_COLUMN = 2;

		public ExtractColumns(Fields fieldDeclaration) {
			super(1, fieldDeclaration);
		}


		@Override
		public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
			TupleEntry args = functionCall.getArguments();
			String line = args.getString(0);

			// Nothing's there
			if (line.length() == 0) {
				return;
			}

			String[] dataLine = line.split(",");

            Tuple result = new Tuple();
            result.add(dataLine[TYPE_COLUMN]);
            functionCall.getOutputCollector().add(result);
		}
	}

	public void run(String[] args) {
		Fields type = new Fields("type");
		Fields text = new Fields("text");

		Tap docTap = new Hfs(new TextLine(text), args[0]);
		Tap wcTap = new Hfs(new TextDelimited(true, "\t"), args[1]);

		Pipe docPipe = new Pipe("type");

		docPipe = new Each(docPipe, text, new ExtractColumns(type), Fields.RESULTS);

		Pipe wcPipe = new Pipe("wc", docPipe);
		wcPipe = new Retain(wcPipe, type);
		wcPipe = new GroupBy(wcPipe, type);
		wcPipe = new Every(wcPipe, Fields.ALL, new Count(), Fields.ALL);

		FlowDef flowDef = FlowDef.flowDef().setName("wc")
			.addSource(docPipe, docTap)
			.addTailSink(wcPipe, wcTap);

		//j.s.nowacki@gmail.com

		Properties properties = AppProps.appProps()
				.setJarClass(HousesByTypeCountHadoop.class)
				.setName("house-count-by-type-cascading")
				.setVersion("1.2.3")
				.buildProperties();

		//properties.put("fs.defaultFS", "hdfs://sandbox:8020");
		//properties.put("hadoop.job.ugi", "hue");

		FlowConnector flowConnector = new Hadoop2MR1FlowConnector(properties);

		Flow flow = flowConnector.connect(flowDef);
		flow.complete();
	}

	public static void main(String[] args) {
		HousesByTypeCountHadoop wc = new HousesByTypeCountHadoop();
		wc.run(args);
	}
}
