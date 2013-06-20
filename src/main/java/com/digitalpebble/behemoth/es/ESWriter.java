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
 **/

package com.digitalpebble.behemoth.es;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Progressable;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.ImmutableSettings.Builder;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.digitalpebble.behemoth.BehemothDocument;

public class ESWriter {

	private static final Logger LOG = LoggerFactory.getLogger(ESWriter.class);

	private Progressable progress;

	TransportClient client;

	public ESWriter(Progressable progress) {
		this.progress = progress;
	}

	public static InetSocketTransportAddress[] parseHostPorts(String[] hostPorts)
			throws IOException {
		InetSocketTransportAddress[] addresses = new InetSocketTransportAddress[hostPorts.length];
		for (int i = 0; i < hostPorts.length; i++) {
			String hp = hostPorts[i];
			String[] tokens = hp.trim().split(":");
			if (tokens.length != 2) {
				LOG.error("Invalid host:port <= " + hp);
				throw new IOException("Invalid host:port <= " + hp);
			}
			addresses[i] = new InetSocketTransportAddress(tokens[0],
					Integer.parseInt(tokens[1]));
		}
		return addresses;
	}

	public void open(JobConf job, String name) throws IOException {
		// get the host and port
		String[] hostPorts = job.getStrings("es.server.hostPort");

		Builder settingsBuilder = ImmutableSettings.settingsBuilder();
		// cluster name
		job.get("es.cluster.name", "elasticsearch");

		// TODO add all the param starting with es. from the config

		client = new TransportClient(settingsBuilder.build());

		for (InetSocketTransportAddress address : parseHostPorts(hostPorts)){
			client.addTransportAddress(address);
		}
	}

	public void write(BehemothDocument doc) throws IOException {
		progress.progress();

		Map<String, Object> json = new HashMap<String, Object>();
		json.put("id", doc.getUrl());
		json.put("text", doc.getText());

		// TODO send in bulks later
		// TODO specify index name and type

		IndexResponse response = client.prepareIndex().setSource(json)
				.execute().actionGet();

		LOG.debug("Adding doc with id\t" + doc.getUrl());

	}

	public void close() throws IOException {
		client.close();
	}

}
