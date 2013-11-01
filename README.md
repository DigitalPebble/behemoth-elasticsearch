ElasticSearch module for Behemoth

See https://github.com/DigitalPebble/behemoth. 

The source of this module is on https://github.com/DigitalPebble/behemoth-elasticsearch

* compile with mvn clean install
* hadoop jar target/behemoth-elasticsearch-1.0-SNAPSHOT-job.jar com.digitalpebble.behemoth.es.ESIndexerJob  -D es.resource=behemoth/docs BEHEMOTH_INPUT

where es.resource is mandatory. See http://www.elasticsearch.org/guide/en/elasticsearch/hadoop/current/configuration.html for the elasticsearch-hadoop configuration.

You can specify 

es.metadata= [true|false]
es.annotations = [true|false]

to convert the metadata and annotations from the Behemoth document into fields for ElasticSearch.

You can also specify a mapping between the annotations and the field names for ES following the syntax es.f.name = BehemothType.featureName

e.g. es.f.person = Person.string 

will map the "string" feature of the annotations of type Person onto the ES field "person".

If a feature name is not set or is *, then string covered by a given annotation is used as value.
