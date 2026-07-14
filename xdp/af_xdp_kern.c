/* SPDX-License-Identifier: GPL-2.0 */
#include <linux/bpf.h>
#include <linux/in.h>
#include <bpf/bpf_helpers.h>
#include <bpf/bpf_endian.h>
#include "../common/parsing_helpers.h"
#include <string.h>

struct {
        __uint(type, BPF_MAP_TYPE_XSKMAP);
        __type(key, __u32);
        __type(value, __u32);
        __uint(max_entries, 4096);
} xsks_map SEC(".maps");

/*
 * Maps (dst IPv4 address, dst TCP port) → XDP socket queue index.
 * Both fields are in network byte order, matching the IP/TCP header
 * values directly.  Populated by userspace via the port lifecycle hook
 * before the first SYN is sent (outbound) or when a listener is bound.
 * Only the destination address/port is looked up; there is no source-port
 * fallback, which avoids steering unrelated traffic that happens to share
 * a source port with one of our registered ports.
 */
struct port_map_key {
        __u32 ip;    /* network byte order */
        __u16 port;  /* network byte order */
        __u16 _pad;
} __attribute__((packed));

struct {
        __uint(type, BPF_MAP_TYPE_HASH);
        __type(key, struct port_map_key);
        __type(value, __u32);
        __uint(max_entries, 4096);
} port_to_queue SEC(".maps");

struct datarec {
	__u64 received;
//	__u64 notfound;
};

struct stats_key {
	__u32 src_ip;
	__u32 dst_ip;
	__u16 src_port;
	__u16 dst_port;
}__attribute__((packed));

struct {
	__uint(type, BPF_MAP_TYPE_HASH);
	__type(key, struct stats_key);
	__type(value, struct datarec);
	__uint(max_entries, 4096);
} xdp_stats_map SEC(".maps");

SEC("xdp")
int xdp_sock_prog(struct xdp_md *ctx)
{
        int action = XDP_PASS;
        void *data_end = (void *)(long)ctx->data_end;
        void *data = (void *)(long)ctx->data;

        struct hdr_cursor nh;
        int nh_type, ip_type = 0;
        struct tcphdr *tcphdr;
        nh.pos = data;

        struct ethhdr *eth;
	struct iphdr *iph;
	struct ipv6hdr *ip6h;

        nh_type = parse_ethhdr(&nh, data_end, &eth);
        if (nh_type < 0){
                bpf_printk("XDP: cannot parse ethernet");
                return XDP_PASS;
        }

        /* Only IPv4 is steered; IPv6 passes through to the kernel. */
        __u32 dst_ip = 0;
	__u32 src_ip = 0;
        if (nh_type == bpf_htons(ETH_P_IPV6)) {
                ip_type = parse_ip6hdr(&nh, data_end, &ip6h);
        } else if (nh_type == bpf_htons(ETH_P_IP)) {
                ip_type = parse_iphdr(&nh, data_end, &iph);
                if (ip_type >= 0){
                        dst_ip = iph->daddr;
			src_ip = iph->saddr;
		}
        }

        if (ip_type == IPPROTO_TCP) {
                if (parse_tcphdr(&nh, data_end, &tcphdr) < 0) {
                        action = XDP_ABORTED;
                        bpf_printk("XDP: cannot parse TCP");
                        goto out;
                }
                __u16 dst = tcphdr->dest;
                __u16 src = tcphdr->source;
		struct port_map_key key;
		__builtin_memset(&key, 0, sizeof(key));
		key.ip = dst_ip;
		key.port = dst;
		struct stats_key skey;
		__builtin_memset(&skey, 0, sizeof(skey));
		skey.dst_ip = dst_ip;
		skey.src_ip = src_ip;
		skey.dst_port = dst;
		skey.src_port = src;
		struct datarec *rec = NULL, newrec;
		newrec.received = 0;
//		newrec.notfound = 0;
		rec = bpf_map_lookup_elem(&xdp_stats_map, &skey);
		__u32 *qid = bpf_map_lookup_elem(&port_to_queue, &key);
		if (qid){
			if (rec){
				rec->received++;
			}else{
				newrec.received++;
				bpf_map_update_elem(&xdp_stats_map, &skey, &newrec, 0);
			}
			return bpf_redirect_map(&xsks_map, ctx->rx_queue_index, XDP_PASS);
		}else{
			key.ip = src_ip;
		       	key.port = src;
			key._pad = 0;
			qid = bpf_map_lookup_elem(&port_to_queue, &key);
			if (qid){
				if (rec){
					rec->received++;
				}else{
					newrec.received++;
					bpf_map_update_elem(&xdp_stats_map, &skey, &newrec, 0);
				}
				return bpf_redirect_map(&xsks_map, ctx->rx_queue_index, XDP_PASS);
			}else{
//				if (rec){
//					rec->notfound++;
//				}else{
//					newrec.notfound++;
//					bpf_map_update_elem(&xdp_stats_map, &skey, &newrec, 0);
//				}
			}
		}
        } else {
		bpf_printk("XDP: non-TCP");
                goto out;
        }
 out:
        return action;
}

char _license[] SEC("license") = "GPL";
