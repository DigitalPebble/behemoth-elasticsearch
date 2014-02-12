package com.digitalpebble.behemoth.es;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.elasticsearch.hadoop.mr.EsOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.digitalpebble.behemoth.BehemothConfiguration;

/**
 * Sends annotated documents to ElasticSearch for indexing
 */

public class ESIndexerJob extends Configured implements Tool {
	private static final Logger LOG = LoggerFactory
			.getLogger(ESIndexerJob.class);

	public ESIndexerJob() {
	}

	public ESIndexerJob(Configuration conf) {
		super(conf);
	}

	public static void main(String args[]) throws Exception {
		int res = ToolRunner.run(BehemothConfiguration.create(),
				new ESIndexerJob(), args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {

		if (args.length != 1) {
			String syntax = "com.digitalpebble.behemoth.ESIndexerJob input";
			System.err.println(syntax);
			return -1;
		}

		Path inputPath = new Path(args[0]);

		JobConf job = new JobConf(getConf());

		job.setJarByClass(this.getClass());

		job.setJobName("Indexing " + inputPath + " into ElasticSearch");

		job.setInputFormat(SequenceFileInputFormat.class);

		job.setMapOutputValueClass(MapWritable.class);

		job.setMapperClass(BehemothToESMapper.class);

		job.setSpeculativeExecution(false); // disable speculative execution
											// when writing to ES

		// job.set("es.resource", "radio/artists"); // index used for storing
		// data
		job.setOutputFormat(EsOutputFormat.class); // use dedicated output
													// format

		FileInputFormat.addInputPath(job, inputPath);

		// no reducer : send straight to elasticsearch at end of mapping
		job.setNumReduceTasks(0);

		try {
			long start = System.currentTimeMillis();
			JobClient.runJob(job);
			long finish = System.currentTimeMillis();
			if (LOG.isInfoEnabled()) {
				LOG.info("ESIndexerJob completed. Timing: " + (finish - start)
						+ " ms");
			}
		} catch (Exception e) {
			LOG.error("Exception while running job", e);
			return -1;
		}
		return 0;
	}
}
