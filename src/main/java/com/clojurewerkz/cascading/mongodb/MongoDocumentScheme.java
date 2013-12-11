package com.clojurewerkz.cascading.mongodb;

import cascading.flow.FlowProcess;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tap.Tap;
import cascading.tuple.Tuple;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.MongoURI;
import com.mongodb.hadoop.io.BSONWritable;
import com.mongodb.hadoop.mapred.MongoInputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.mongodb.hadoop.util.MongoConfigUtil;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Scheme that creates 1-tuples of the mongo document
 */
public class MongoDocumentScheme extends MongoScheme {

    private static final Logger logger = LoggerFactory.getLogger(MongoDbCollector.class);

    private final String pathUUID = UUID.randomUUID().toString();
    private String mongoUri;
    private String mongoAuthUri; // for calculating splits

    private List<String> fieldsToInclude;

    private String host;
    private Integer port;
    private String database;
    private String collection;

    private Map<String, Object> query = null;
    private Integer splitSize = null;


    public MongoDocumentScheme(String host, Integer port, String database, String collection, List<String> fieldsToInclude) {
        this(host, port, database, collection, fieldsToInclude, null);
    }

    public MongoDocumentScheme(String host, Integer port, String database, String collection, List<String> fieldsToInclude, Map<String, Object> query) {
        this(host,port,null,null,database,collection,fieldsToInclude,query, null, null);
    }

    public MongoDocumentScheme(String host, Integer port, String username, String password, String database, String collection, List<String> fieldsToInclude, Map<String, Object> query, Integer splitSize, String authDb) {
        this.fieldsToInclude = fieldsToInclude;

        if(username != null && password != null){
            if(authDb != null){
                this.mongoAuthUri = String.format("mongodb://%s:%s@%s:%d/%s", username, password, host, port, authDb);
            }
            this.mongoUri =     String.format("mongodb://%s:%s@%s:%d/%s.%s", username, password, host, port, database, collection);
        }else{
            this.mongoUri = String.format("mongodb://%s:%d/%s.%s", host, port, database, collection);
        }

        this.host = host;
        this.port = port;
        this.database = database;
        this.collection = collection;
        this.query = query;
        this.splitSize = splitSize;
    }

    @Override
    public String getIdentifier(){
        return String.format("%s_%d_%s_%s", this.host, this.port, this.database, this.collection);
    }

    @Override
    public void sourceConfInit(FlowProcess<JobConf> process, Tap<JobConf, RecordReader, OutputCollector> tap, JobConf conf) {
        MongoConfigUtil.setInputURI( conf, new MongoURI(this.mongoUri) );
        if(this.mongoAuthUri != null){
            MongoConfigUtil.setAuthURI( conf, mongoAuthUri );
        }

        FileInputFormat.setInputPaths(conf, this.getIdentifier());
        conf.setInputFormat(MongoInputFormat.class);

        if(this.fieldsToInclude != null){
            DBObject fields = new BasicDBObject();

            for(String fieldName: this.fieldsToInclude){
                fields.put(fieldName, 1);
            }

            MongoConfigUtil.setFields(conf, fields);
        }

        if(this.query != null){
            MongoConfigUtil.setQuery(conf, new BasicDBObject(query));
        }

        if(this.splitSize != null){
            MongoConfigUtil.setReadSplitsFromShards(conf, true);
            MongoConfigUtil.setCreateInputSplits(conf, true);
            MongoConfigUtil.setReadSplitsFromSecondary(conf, true);
            MongoConfigUtil.setSplitSize(conf, this.splitSize);
        }else{
            MongoConfigUtil.setReadSplitsFromShards(conf, false);
            MongoConfigUtil.setCreateInputSplits(conf, false);
            MongoConfigUtil.setReadSplitsFromSecondary(conf, false);
        }
    }

    @Override
    public boolean source(FlowProcess<JobConf> process, SourceCall<BSONWritable[], RecordReader> sourceCall) throws IOException {
        Tuple result = new Tuple();

        BSONWritable key = sourceCall.getContext()[0];
        BSONWritable value = sourceCall.getContext()[1];

        if (!sourceCall.getInput().next(key, value)) {
            logger.info("Nothing left to read, exiting");
            return false;
        }

        result.add(value.getDoc());
        sourceCall.getIncomingEntry().setTuple(result);
        return true;
    }


    @Override
    public void sourcePrepare(FlowProcess<JobConf> flowProcess, SourceCall<BSONWritable[], RecordReader> sourceCall) {
        RecordReader input = sourceCall.getInput();
        sourceCall.setContext(
            new BSONWritable[]{
                (BSONWritable)input.createKey(),
                (BSONWritable)input.createValue(),
            }
        );
    }

    @Override
    public Path getPath() {
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public void sinkConfInit(FlowProcess<JobConf> jobConfFlowProcess, Tap<JobConf, RecordReader, OutputCollector> jobConfRecordReaderOutputCollectorTap, JobConf entries) {
        throw new UnsupportedOperationException("Sink Mode Not Implemented.");
    }

    @Override
    public void sink(FlowProcess<JobConf> jobConfFlowProcess, SinkCall<BSONWritable[], OutputCollector> outputCollectorSinkCall) throws IOException {
        throw new UnsupportedOperationException("Sink Mode Not Implemented.");
    }
}
