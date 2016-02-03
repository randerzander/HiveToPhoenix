create external table one(id string, val int)
row format delimited fields terminated by ','
location '${hiveconf:PATH}'
;
create external table two(id string, val int)
row format delimited fields terminated by ','
location '${hiveconf:PATH}'
;
create external table three(id string, val int)
row format delimited fields terminated by ','
location '${hiveconf:PATH}'
;
