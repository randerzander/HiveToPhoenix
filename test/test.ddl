create external table test(id string, val int)
row format delimited fields terminated by ','
location '${hiveconf:PATH}'
;
