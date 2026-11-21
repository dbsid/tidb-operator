# tidb-one

tidb-one runs TiDB and TiKV in one process (`tidb-one-server`). A TiProxy
locality router sends each transaction to the server next to the leader of
the data it touches. This example deploys 3 PDs, 3 TiDBOne servers and 2
TiDBOneRouters.

```sh
kubectl create ns one
kubectl -n one apply -f examples/tidb-one/
# Clients connect to the routers:
kubectl -n one port-forward svc/router-tidb-one-router 6000:6000
mysql -h 127.0.0.1 -P 6000 -u root
```

## Components

| Group | Instance | Pod runs | Services |
|---|---|---|---|
| `TiDBOneGroup` | `TiDBOne` | `tidb-one-server`: TiDB (4000, status 10080) and TiKV (20160, status 20180) | `<group>-tidb-one-peer` (headless) |
| `TiDBOneRouterGroup` | `TiDBOneRouter` | `tiproxy-rs locality-router` (6000, status 3080) | `<group>-tidb-one-router` (clients), `<group>-tidb-one-router-seed` (TiDB status ports, read by the routers at start) |

`spec.template.spec.config` of a TiDBOneGroup is the TiDB config and
`tikvConfig` the config of the embedded TiKV. Addresses, PD and the data
directory are set by the operator as flags.

The images default to `ghcr.io/dbsid/tidb-one-server` and
`ghcr.io/dbsid/tiproxy-rs`, tagged with `version`.

## Session token signing cert

A router moves a session from one server to another with a session token,
like TiProxy. All servers must sign the tokens with the same cert. Create a
TLS secret and reference it from the Cluster:

```sh
openssl req -x509 -newkey rsa:2048 -nodes -days 3650 -subj "/CN=tidb-one-session" \
  -keyout tls.key -out tls.crt
kubectl -n one create secret tls session-token --cert=tls.crt --key=tls.key
kubectl -n one patch cluster one --type merge \
  -p '{"spec":{"security":{"sessionTokenSigningCertKeyPair":{"name":"session-token"}}}}'
```

Without the cert, the routers cannot move sessions: a restarted server's
sessions wait for the drain timeout and are then closed.

## Restart, upgrade, scale-in

When a TiDBOne pod must be recreated (version, config with the `Restart`
strategy, resources, overlay, or the signing cert changed), the operator
handles one instance at a time:

1. Evict the leaders of its store (PD evict-leader scheduler), as for TiKV.
2. Drain it on every router (`POST /api/backends/drain?addr=` on the router
   status port). The router sends no new transaction to it and moves each
   session to another server at the next transaction boundary. The
   `RoutersDrained` condition becomes true when no router has a connection
   to it, or after `drainTimeoutSeconds` (60 by default).
3. Delete the pod; the new one starts with the same PVC.
4. When the new pod is ready, resume it on the routers and remove the evict
   scheduler.

Scale-in (lowering `replicas`) drains the server after its leaders are
evicted, then takes the store offline; the pod is deleted when the store
is `Tombstone`.

Router rolling updates start a new router before deleting an old one. A
deleted router closes its client connections.

## Monitoring

Each TiDBOne pod is a TiDB server and a TiKV store:

- `prometheus.io/port` is the TiDB status port.
- `tikv.prometheus.io/port` is the TiKV status port.

TiDB Dashboard (PD) lists the servers as TiDB instances and as TiKV
stores. The stock TiDB and TiKV Grafana dashboards filter on
`job="tidb"` and `job=~".*tikv"`. With Prometheus Operator, add a
PodMonitor that scrapes both ports with those job names (next to the
PodMonitor of [Deploy Monitoring and Alerts](https://docs.pingcap.com/tidb-in-kubernetes/v2.0/monitor-a-tidb-cluster/)):

```yaml
apiVersion: monitoring.coreos.com/v1
kind: PodMonitor
metadata:
  name: tidb-one
  labels:
    monitor: tidb-cluster
spec:
  selector:
    matchLabels:
      app.kubernetes.io/managed-by: tidb-operator
      app.kubernetes.io/component: tidb-one
  podMetricsEndpoints:
  # TiDB status port, job "tidb"
  - interval: 15s
    honorLabels: true
    relabelings:
    - sourceLabels: [__meta_kubernetes_pod_ip, __meta_kubernetes_pod_annotation_prometheus_io_port]
      regex: (.+);(.+)
      replacement: $1:$2
      targetLabel: __address__
    - replacement: tidb
      targetLabel: job
    - sourceLabels: [__meta_kubernetes_namespace, __meta_kubernetes_pod_label_app_kubernetes_io_instance]
      separator: '-'
      targetLabel: tidb_cluster
    - sourceLabels: [__meta_kubernetes_pod_name]
      targetLabel: instance
  # TiKV status port, job "tikv"
  - interval: 15s
    honorLabels: true
    relabelings:
    - sourceLabels: [__meta_kubernetes_pod_ip, __meta_kubernetes_pod_annotation_tikv_prometheus_io_port]
      regex: (.+);(.+)
      replacement: $1:$2
      targetLabel: __address__
    - replacement: tikv
      targetLabel: job
    - sourceLabels: [__meta_kubernetes_namespace, __meta_kubernetes_pod_label_app_kubernetes_io_instance]
      separator: '-'
      targetLabel: tidb_cluster
    - sourceLabels: [__meta_kubernetes_pod_name]
      targetLabel: instance
```

Routers export `tiproxy_locality_*` metrics on `prometheus.io/port`.

## Limits

- TLS is not supported: a cluster with TLS enabled is rejected.
- Router restarts close client connections.
