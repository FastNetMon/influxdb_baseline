#!/usr/bin/python3

from influxdb import InfluxDBClient

# Process data for last X hours
time_period = '72h'

all_metrics = [ 'packets_incoming', 'bits_incoming', 'flows_incoming', 'packets_outgoing', 'bits_outgoing', 'flows_outgoing' ]

per_protocol_incoming = ['fragmented_packets_incoming', 'tcp_packets_incoming', 'tcp_syn_packets_incoming', 'udp_packets_incoming', 'icmp_packets_incoming', 'fragmented_bits_incoming', 'tcp_bits_incoming', 'tcp_syn_bits_incoming', 'udp_bits_incoming', 'icmp_bits_incoming' ]

per_protocol_outgoing = ['fragmented_packets_outgoing', 'tcp_packets_outgoing', 'tcp_syn_packets_outgoing', 'udp_packets_outgoing', 'icmp_packets_outgoing', 'fragmented_bits_outgoing', 'tcp_bits_outgoing', 'tcp_syn_bits_outgoing', 'udp_bits_outgoing', 'icmp_bits_outgoing']

# Switch to True if you need per protocol thresholds too
process_per_protocol_counters = False 

if process_per_protocol_counters:
   all_metrics.extend(per_protocol_incoming)
   all_metrics.extend(per_protocol_outgoing)

client = InfluxDBClient('localhost', 8086, 'root', 'root', 'fastnetmon')

all_hosts = client.query('show tag values from hosts_traffic with key = "host"')

all_hosts_dict = {}

for point in all_hosts.get_points():
        all_hosts_dict[ point['value'] ] = 1


print ("Extracted", len(all_hosts_dict), "hosts from InfluxDB")

query_select_fields = []

for metric in all_metrics:
    query_select_fields.append( "max(" + metric + ") as max_" + metric + " " )

query_select_field = ",".join(query_select_fields)

hosts_to_process = all_hosts_dict.keys()

peak_values_across_al_hosts = {}

for metrics in all_metrics:
    peak_values_across_al_hosts[ "max_" + metrics] = 0

# Number of hosts with any availible metrics for specified time range
non_empty_host = 0

for host in sorted(hosts_to_process):
    host_metrics = client.query("SELECT " + query_select_field + " FROM hosts_traffic WHERE host = '"+ host + "' AND time >= now() - " + time_period)
   
    points = list(host_metrics.get_points())

    if len(points) == 0:
        # print "Skip blank data for", host
        continue

    non_empty_host += 1

    for k, v in points[0].items():
        if k == "time":
            continue

        if peak_values_across_al_hosts[k] < v:
            peak_values_across_al_hosts[k] = v
        # print k, v

print ("Number of hosts with any metrics availible", non_empty_host)
print ("Peak values for all your hosts")
for k, v in peak_values_across_al_hosts.items():
    print ("%-20s %s" % (k, v))
