package djr.storm.strident;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.trident.operation.builtin.Debug;
import org.apache.storm.trident.testing.Split;
import org.apache.storm.tuple.Fields;

public class TridentWordCountDriver {
	
	public static void main(String[] argv){

        if(argv.length != 1){
            System.out.println("Please provide input file path");
            System.exit(-1);
        }

        String inputFile = argv[0];
        System.out.println("TridentWordCountDriver is starting for " + inputFile);
        Config config = new Config();
        config.put("inputFile",inputFile);

        LocalDRPC localDRPC = new LocalDRPC();
        LocalCluster localCluster = new LocalCluster();

        LineReaderSpout lineReaderSpout = new LineReaderSpout();
        TridentTopology topology = new TridentTopology();
        topology.newStream("spout",lineReaderSpout)
                .each(new Fields("line"), new Split(), new Fields("word"))
                .groupBy(new Fields("word"))
                .aggregate(new Fields("word"), new Count(), new Fields("count"))
                .each(new Fields("word","count"), new Debug());

        localCluster.submitTopology("WordCount",config,topology.build());;
        
        //localDRPC.shutdown();
        //localCluster.shutdown();
    }

}
