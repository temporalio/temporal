Use development.yaml file to override the default dynamic config value (they are specified
when creating the service config).

Each key can have zero or more values and each value can have zero or more
constraints. There are only three types of constraint:
    1. domainName: string
    2. taskListName: string
    3. taskType: int (0:Decision, 1:Activity)
A value will be selected and returned if all its has exactly the same constraints
as the ones specified in query filters (including the number of constraints).

Please use the following format:
```
testGetBoolPropertyKey:
  - value: false
  - value: true
    constraints:
      domainName: "global-samples-domain"
  - value: false
    constraints:
      domainName: "samples-domain"
testGetDurationPropertyKey:
  - value: "1m"
    constraints:
      domainName: "samples-domain"
      taskListName: "longIdleTimeTasklist"
testGetFloat64PropertyKey:
  - value: 12.0
    constraints:
      domainName: "samples-domain"
testGetMapPropertyKey:
  - value:
      key1: 1
      key2: "value 2"
      key3:
        - false
        - key4: true
          key5: 2.0
```
