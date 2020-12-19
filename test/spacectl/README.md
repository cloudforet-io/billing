# example

~~~
spacectl exec get_data billing.Billing -p start="2019-10-01" -p end="2020-12-17" -p granularity="MONTHLY" -j '{"aggregation": ["RESOURCE_TYPE"], "limit": 2, "sort": {"date":"2020-11","desc":true}}'
~~~
