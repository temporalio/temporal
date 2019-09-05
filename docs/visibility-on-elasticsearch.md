# Overview
Cadence visibility APIs allow users to list open or closed workflows with filters such as WorkflowType or WorkflowID.   
With Cassandra, there are issues around scalability and performance, for example: 
 - list large amount of workflows may kill Cassandra node.
 - data is partitioned by domain, which means writing large amount of workflow to one domain will cause Cassandra nodes hotspots.
 - query with filter is slow for large data.

(With MySQL, there might be similar issues but not tested)

In addition, Cadence want to support users to perform flexible query with multiple filters on even custom info.  
That's why Cadence add support for enhanced visibility features on top of ElasticSearch (Note as ES below), which includes: 
 - new visibility APIs to List/Scan/Count workflows with SQL like query
 - search attributes feature to support users provide custom info
 
# Quick Start
## Local Cadence Docker Setup
1. Increase docker memory to higher 6GB. Docker -> Preference -> advanced -> memory limit
2. Get docker compose file. Run `curl -O https://raw.githubusercontent.com/uber/cadence/master/docker/docker-compose-es.yml`
3. Start cadence docker which contains Kafka, Zookeeper and ElasticSearch. Run `docker-compose -f docker-compose-es.yml up`
4. From docker output log, make sure ES and cadence started correctly. If encounter disk space not enough, try `docker system prune -a --volumes`
5. Register local domain and start using it. `cadence --do samples-domain d re`
 

## CLI Search Attributes Support 

Make sure Cadence CLI version is 0.6.4+

### new list API examples

```
cadence --do samples-domain wf list -q 'WorkflowType = "main.Workflow" and (WorkflowID = "1645a588-4772-4dab-b276-5f9db108b3a8" or RunID = "be66519b-5f09-40cd-b2e8-20e4106244dc")'
cadence --do samples-domain wf list -q 'WorkflowType = "main.Workflow" StartTime > "2019-06-07T16:46:34-08:00" and CloseTime = missing'
```
To list only open workflows, add `CloseTime = missing` to the end of query.  

### start workflow with search attributes 

```
cadence --do samples-domain workflow start --tl helloWorldGroup --wt main.Workflow --et 60 --dt 10 -i '"input arg"' -search_attr_key 'CustomIntField | CustomKeywordField | CustomStringField |  CustomBoolField | CustomDatetimeField' -search_attr_value '5 | keyword1 | my test | true | 2019-06-07T16:16:36-08:00'
```

Note: start workflow with search attributes but without ES will succeed as normal, but will not be searchable and will not be shown in list result.

### search workflow with new list API 

```
cadence --do samples-domain wf list -q '(CustomKeywordField = "keyword1" and CustomIntField >= 5) or CustomKeywordField = "keyword2"' -psa
cadence --do samples-domain wf list -q 'CustomKeywordField in ("keyword2", "keyword1") and CustomIntField >= 5 and CloseTime between "2018-06-07T16:16:36-08:00" and "2019-06-07T16:46:34-08:00" order by CustomDatetimeField desc' -psa
```

(Search attributes can be updated inside workflow, see example [here](https://github.com/uber-common/cadence-samples/tree/master/cmd/samples/recipes/searchattributes).

# Details
## Dependencies
- Zookeeper - for Kafka to start
- Kafka - message queue for visibility data 
- ElasticSearch v6+ - for data search (early ES version may not support some queries)

## Configuration
```
persistence:
  ...
  advancedVisibilityStore: es-visibility
  ...
  datastore:
    es-visibility:
      elasticsearch:
        url:
          scheme: "http"
          host: "127.0.0.1:9200"
        indices:
          visibility: cadence-visibility-dev
```
This part is used to config advanced visibility store to ElasticSearch. 
 - `url` is for Cadence to discover ES 
 - `indices/visibility` is ElasticSearch index name for the deployment.  

```
kafka:
  ...
  applications:
    visibility:
      topic: cadence-visibility-dev
      dlq-topic: cadence-visibility-dev-dlq
  ...
``` 
Also need to add a kafka topic to visibility, see above for example.  

There are dynamic configs to control ElasticSearch visibility features:
- `system.advancedVisibilityWritingMode` is an int property to control how to write visibility to data store.  
`"off"` means do not write to advanced data store,   
`"on"` means only write to advanced data store,   
`"dual"` means write to both DB (Cassandra or MySQL) and advanced data store
- `system.enableReadVisibilityFromES` is a boolean property to control whether Cadence List APIs should use ES as source or not.

