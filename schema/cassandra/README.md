What
----
This directory contains the cassandra schema for every keyspace that temporal owns. The directory structure is as follows

```
./schema
   - keyspace1/
   - keyspace2/
        - keyspace.cql     -- Contains the keyspace definition
        - schema.cql       -- Contains the latest & greatest snapshot of the schema for the keyspace
        - versioned
             - v0.1/
             - v0.2/       -- One directory per schema version change
             - v1.0/
                - manifest.json   -- json file describing the change
                - changes.cql     -- changes in this version, only [CREATE, ALTER] commands are allowed
```

How
---

Q: How do I update existing schema ?
* Add your changes to schema.cql
* Create a new schema version directory under ./schema/keyspace/versioned/vx.x
  * Add a manifest.json
  * Add your changes in a cql file
* Once you are done with these use the ./temporal-cassandra-tool to update the schema

Q: What's the format of manifest.json

Example below:
* MinCompatibleVersion is the minimum schema version that your code can handle
* SchemaUpdateCqlFiles are list of .cql files containing your create/alter commands


```
{
    "CurrVersion": "0.1",
    "MinCompatibleVersion": "0.1",
    "Description": "base version of schema",
    "SchemaUpdateCqlFiles": [
        "base.cql"
    ]
}
```