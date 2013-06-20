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

import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.digitalpebble.behemoth.BehemothConfiguration;
import com.digitalpebble.behemoth.BehemothDocument;

/**
 * Sends annotated documents to ElasticSearch for indexing
 */

public class ESIndexerJob extends Configured implements Tool {
	private static final Log LOG = LogFactory.getLog(ESIndexerJob.class);

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

		final FileSystem fs = FileSystem.get(getConf());

		if (args.length != 2) {
			String syntax = "com.digitalpebble.solr.ESIndexerJob input host:port";
			System.err.println(syntax);
			return -1;
		}

		Path inputPath = new Path(args[0]);
		String hostPort = args[1];
		
		int numAddresses = ESWriter.parseHostPorts(hostPort.split(" *, *")).length;
		if (numAddresses == 0){
			String syntax = "Must specify a list of comma separated 'host:port'";
			System.err.println(syntax);
			return -1;
		}

		JobConf job = new JobConf(getConf());

		job.setJarByClass(this.getClass());

		job.setJobName("Indexing " + inputPath + " into ElasticSearch");

		job.setInputFormat(SequenceFileInputFormat.class);
		job.setOutputFormat(ESOutputFormat.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(BehemothDocument.class);

		job.setMapperClass(IdentityMapper.class);
		// no reducer : send straight to elasticsearch at end of mapping
		job.setNumReduceTasks(0);

		FileInputFormat.addInputPath(job, inputPath);
		final Path tmp = new Path("tmp_" + System.currentTimeMillis() + "-"
				+ new Random().nextInt());
		FileOutputFormat.setOutputPath(job, tmp);

		job.set("es.server.hostPort", hostPort);

		try {
			long start = System.currentTimeMillis();
			JobClient.runJob(job);
			long finish = System.currentTimeMillis();
			if (LOG.isInfoEnabled()) {
				LOG.info("ESIndexerJob completed. Timing: " + (finish - start)
						+ " ms");
			}
		} catch (Exception e) {
			LOG.error(e);
		} finally {
			fs.delete(tmp, true);
		}

		return 0;
	}
}