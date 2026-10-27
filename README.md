# k8s-jetstream-ufs

Mount a [jetstream](https://jetstream.rs) 9P filesystem in Kubernetes.

- `k8s-jetstream-ufs serve` exports a directory over 9P2000.L using `jetstream_ufs` (TCP, optional unix socket).
- `k8s-jetstream-ufs csi` is a CSI node plugin (`ufs.jetstream.rs`) that mounts an export into pods with the kernel
  `9p` client (`mount -t 9p -o trans=tcp,version=9p2000.L`).

The kernel 9p client has no QUIC/iroh transport, so the node plugin speaks plain TCP to the server; run it over a
trusted pod network.

## Deploy

```sh
docker build -t k8s-jetstream-ufs:dev .
kubectl apply -f deploy/server.yaml      # one export (Deployment + Service)
kubectl apply -f deploy/csi-driver.yaml  # CSIDriver + node DaemonSet
kubectl apply -f deploy/example.yaml     # PV/PVC/Pod using volumeAttributes {server, port}
```

Dynamic provisioning: `kubectl apply -f deploy/storageclass.yaml`, then create PVCs with `storageClassName: ufs`
(see `deploy/example-dynamic.yaml`). Each PVC gets its own directory on the export (`subpath` volume attribute →
9P attach name); deleting the PVC deletes the directory. The controller runs next to the server and shares its
`/export` volume.

Nodes need the `9p` and `9pnet` kernel modules. Static PVs may set `subpath` in `volumeAttributes` to mount a subdirectory of the export.

## Tests

- `cargo test`: unit tests plus a protocol e2e test that drives the server over TCP with a raw 9P client.
- `hack/e2e.sh` (CI job `kind-e2e`): builds the image, starts a kind cluster, deploys the server and driver, and
  checks that pods read and write the export through a real kernel 9p mount.
