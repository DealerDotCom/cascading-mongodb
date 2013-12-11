package com.clojurewerkz.cascading.mongodb;

import cascading.scheme.Scheme;
import com.mongodb.hadoop.io.BSONWritable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;

public abstract class MongoScheme extends Scheme<JobConf, RecordReader, OutputCollector, BSONWritable[], BSONWritable[]> {

    public abstract String getIdentifier();
    public abstract Path getPath();
}
