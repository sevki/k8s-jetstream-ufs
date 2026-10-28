# k8s-jetstream-ufs

Mount a [jetstream](https://jetstream.rs) 9P filesystem in Kubernetes, over **QUIC (mTLS)** or **iroh**.

- `k8s-jetstream-ufs serve` exports a directory over 9P2000.L (`jetstream_ufs`) on a QUIC endpoint and/or an iroh
  endpoint. There is no TCP listener.
- `k8s-jetstream-ufs csi` is a CSI plugin (`ufs.jetstream.rs`). As a node plugin it mounts exports into pods with the
  kernel `9p` client; with `--controller-export-root` it is the controller that creates and deletes per-volume
  directories for dynamic provisioning.

The kernel 9p client can only talk TCP or unix sockets. So for every mount the node plugin opens a local unix-socket
proxy, points `mount -t 9p -o trans=unix` at it, and carries the byte stream to the server as a QUIC or iroh
bidirectional stream. Nothing 9P-related crosses the pod network in the clear.

```
pod ── kernel 9p ── unix socket ── node plugin ══ QUIC (mTLS) / iroh ══ k8s-jetstream-ufs serve ── /export
```

Limitation: a mount lives as long as the node plugin process. Restarting the DaemonSet pod breaks existing mounts
(the kernel 9p client does not reconnect); restart the affected workloads.

## Deploy

```sh
docker build -t k8s-jetstream-ufs:dev .
hack/gen-certs.sh certs                                   # CA, server cert, client cert (use your own PKI in production)
kubectl apply -f deploy/namespace.yaml
kubectl -n jetstream-ufs create secret generic ufs-tls \
  --from-file=ca.crt=certs/ca.crt --from-file=tls.crt=certs/server.crt --from-file=tls.key=certs/server.key
docker run --rm -v "$PWD:/out" k8s-jetstream-ufs:dev iroh-keygen --out /out/iroh.key   # prints the node id
kubectl -n jetstream-ufs create secret generic ufs-iroh --from-file=key=iroh.key
kubectl -n kube-system create secret generic ufs-csi-client-tls \
  --from-file=ca.crt=certs/ca.crt --from-file=tls.crt=certs/client.crt --from-file=tls.key=certs/client.key
kubectl apply -f deploy/server.yaml deploy/csi-driver.yaml deploy/storageclass.yaml
kubectl apply -f deploy/example-dynamic.yaml
```

- **QUIC**: the server requires client certificates (`--client-ca`). The node plugin presents the
  `ufs-csi-client-tls` identity and verifies the server against `ca.crt`; the `server` volume attribute is the TLS
  server name, so it must match a SAN of the server certificate.
- **iroh**: edit `deploy/storageclass-iroh.yaml` with the node id from `iroh-keygen`. `iroh-addrs` gives direct
  addresses; without it iroh discovery and relays are used.

Dynamic provisioning gives each PVC its own directory on the export (`subpath` volume attribute → 9P attach name);
deleting the PVC deletes the directory. Static PVs set `transport` (`quic`|`iroh`), `server`/`port` or
`iroh-node`/`iroh-addrs`, and optionally `subpath` (see `deploy/example.yaml`).

Nodes need the `9p` and `9pnet` kernel modules.

## Tests

- `cargo test`: unit tests plus protocol e2e tests that drive a real server with a raw 9P client over its unix
  socket, over QUIC with mTLS (including that a client without a certificate is refused), and over iroh, the latter
  two through the same dialer and proxy the node plugin uses.
- `hack/e2e.sh` (CI job `kind-e2e`): builds the image, starts a kind cluster, deploys the server and driver with
  generated mTLS material and an iroh key, and checks that pods read and write the export through a real kernel 9p
  mount, statically provisioned, dynamically provisioned over QUIC, and dynamically provisioned over iroh.
