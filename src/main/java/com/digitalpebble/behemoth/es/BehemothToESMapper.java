package com.digitalpebble.behemoth.es;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.elasticsearch.hadoop.util.WritableUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.digitalpebble.behemoth.Annotation;
import com.digitalpebble.behemoth.BehemothDocument;

public class BehemothToESMapper extends MapReduceBase implements
		Mapper<Text, BehemothDocument, Writable, Writable> {

	private static final Logger LOG = LoggerFactory
			.getLogger(BehemothToESMapper.class);

	private boolean includeMetadata;
	private boolean includeAnnotations;

	private Map<String, Map<String, String>> fieldMapping = new HashMap<String, Map<String, String>>();

	private void populateMapping(JobConf job) {
		// get the Behemoth annotations types and features
		// to store as ES fields
		// es.f.name = BehemothType.featureName
		// e.g. es.f.person = Person.string will map the "string" feature
		// of
		// "Person" annotations onto the ES field "person"
		Iterator<Entry<String, String>> iterator = job.iterator();
		while (iterator.hasNext()) {
			Entry<String, String> entry = iterator.next();
			if (entry.getKey().startsWith("es.f.") == false)
				continue;
			String solrFieldName = entry.getKey().substring("es.f.".length());
			String value = entry.getValue();
			// see if a feature has been specified
			// if not we'll use '*' to indicate that we want
			// the text covered by the annotation
			// HashMap<String, String> featureValMap = new HashMap<String,
			// String>();

			String[] toks = value.split("\\.");
			String annotationName = null;
			String featureName = null;
			if (toks.length == 1) {
				annotationName = toks[0];
			} else if (toks.length == 2) {
				annotationName = toks[0];
				featureName = toks[1];
			} else {
				LOG.warn("Invalid annotation field mapping: " + value);
			}

			Map<String, String> featureMap = fieldMapping.get(annotationName);
			if (featureMap == null) {
				featureMap = new HashMap<String, String>();
			}

			if (featureName == null)
				featureName = "*";

			featureMap.put(featureName, solrFieldName);
			fieldMapping.put(annotationName, featureMap);
			LOG.info("Adding mapping for annotation " + annotationName
					+ ", feature '" + featureName + "' to  ES field '"
					+ solrFieldName + "'");
		}
	}

	@Override
	public void configure(JobConf conf) {
		includeMetadata = conf.getBoolean("solr.metadata", true);
		includeAnnotations = conf.getBoolean("solr.annotations", true);
		if (includeAnnotations)
			populateMapping(conf);
	}

	@Override
	public void map(Text key, BehemothDocument value,
			OutputCollector<Writable, Writable> collector, Reporter reporter)
			throws IOException {

		Map<String, String> entry = new LinkedHashMap<String, String>();

		if (StringUtils.isNotBlank(value.getText()))
			entry.put("text", value.getText());

		if (StringUtils.isNotBlank(value.getUrl()))
			entry.put("url", value.getUrl());

		if (StringUtils.isNotBlank(value.getContentType()))
			entry.put("contentType", value.getContentType());

		// metadata
		MapWritable mw = value.getMetadata();
		if (mw != null && includeMetadata) {
			Iterator<Entry<Writable, Writable>> iterator = mw.entrySet()
					.iterator();
			while (iterator.hasNext()) {
				Entry<Writable, Writable> md = iterator.next();
				entry.put(md.getKey().toString(), md.getValue().toString());
			}
		}

		// annotations
		if (includeAnnotations) {
			Iterator<Annotation> iterator = value.getAnnotations().iterator();
			while (iterator.hasNext()) {
				Annotation current = iterator.next();
				// check whether it belongs to a type we'd like to send to
				// ES
				Map<String, String> featureField = fieldMapping.get(current
						.getType());
				if (featureField == null) {
					continue;
				}

				// iterate on the expected features
				for (String targetFeature : featureField.keySet()) {
					String ESFieldName = featureField.get(targetFeature);
					String fieldvalue = null;
					// special case for covering text
					if ("*".equals(targetFeature)) {
						fieldvalue = value.getText().substring(
								(int) current.getStart(),
								(int) current.getEnd());
					}
					// get the value for the feature
					else {
						fieldvalue = current.getFeatures().get(targetFeature);
					}
					LOG.debug("Adding field : " + ESFieldName + "\t" + value);
					// skip if no value has been found
					if (value != null)
						entry.put(ESFieldName, fieldvalue);
				}

			}
		}

		collector.collect(key, WritableUtils.toWritable(entry));
	}

}
