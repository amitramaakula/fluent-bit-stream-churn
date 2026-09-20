function partition(tag, timestamp, record)
 local k = record['kubernetes']
 if k == nil or k['pod_name'] == nil then return -1, timestamp, record end
 local name = k['pod_name']
 local hash = 0
 for i = 1, #name do hash = (hash * 31 + string.byte(name, i)) % 32 end
 record['bounded_stream'] = string.format('reused-logs-%02d', hash)
 record['cluster_name'] = 'local-rollout-demo'
 return 1, timestamp, record
end
