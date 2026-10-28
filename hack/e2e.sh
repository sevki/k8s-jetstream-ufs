#!/usr/bin/env bash
# End-to-end test on a kind cluster. Requires docker, kind, kubectl and the 9p kernel module on the host.
set -euo pipefail
cd "$(dirname "$0")/.."

CLUSTER=${CLUSTER:-ufs-e2e}
IMAGE=k8s-jetstream-ufs:dev

cleanup() { [ -n "${KEEP:-}" ] || kind delete cluster --name "$CLUSTER" >/dev/null 2>&1 || true; }
trap cleanup EXIT

dump() {
  echo "--- diagnostics"
  kubectl get pods -A -o wide || true
  kubectl describe pod ufs-writer || true
  kubectl -n kube-system logs ds/ufs-csi-node -c driver --tail=100 || true
  kubectl -n jetstream-ufs logs deploy/ufs --all-containers --tail=100 || true
}
trap 'dump' ERR

docker build -t "$IMAGE" .
kind create cluster --name "$CLUSTER" --wait 120s
kind load docker-image "$IMAGE" --name "$CLUSTER"

# mTLS material for QUIC, and an iroh identity for the server.
WORK=$(mktemp -d)
hack/gen-certs.sh "$WORK/certs"
IROH_NODE=$(docker run --rm -v "$WORK:/out" "$IMAGE" iroh-keygen --out /out/iroh.key)
echo "iroh node id: $IROH_NODE"

kubectl apply -f deploy/namespace.yaml
kubectl -n jetstream-ufs create secret generic ufs-tls \
  --from-file=ca.crt="$WORK/certs/ca.crt" --from-file=tls.crt="$WORK/certs/server.crt" \
  --from-file=tls.key="$WORK/certs/server.key"
kubectl -n jetstream-ufs create secret generic ufs-iroh --from-file=key="$WORK/iroh.key"
kubectl -n kube-system create secret generic ufs-csi-client-tls \
  --from-file=ca.crt="$WORK/certs/ca.crt" --from-file=tls.crt="$WORK/certs/client.crt" \
  --from-file=tls.key="$WORK/certs/client.key"

kubectl apply -f deploy/server.yaml
kubectl -n jetstream-ufs rollout status deploy/ufs --timeout=120s
kubectl -n jetstream-ufs exec deploy/ufs -c ufs -- sh -c 'echo seed-data > /export/seed.txt'

kubectl apply -f deploy/csi-driver.yaml
kubectl -n kube-system rollout status ds/ufs-csi-node --timeout=180s

kubectl apply -f deploy/example.yaml
kubectl wait --for=jsonpath='{.status.phase}'=Succeeded pod/ufs-writer --timeout=180s

# The pod read the file seeded on the server...
test "$(kubectl logs ufs-writer)" = "seed-data"
# ...and what it wrote landed in the server's export directory.
test "$(kubectl -n jetstream-ufs exec deploy/ufs -c ufs -- cat /export/written.txt)" = "from-writer"

# A second pod on the same volume sees the data (ReadWriteMany) and unmount works on delete.
kubectl delete pod ufs-writer --wait
kubectl run ufs-reader --image=busybox:1.36 --restart=Never --overrides='{
  "spec":{"containers":[{"name":"r","image":"busybox:1.36","command":["cat","/data/written.txt"],
  "volumeMounts":[{"name":"d","mountPath":"/data"}]}],
  "volumes":[{"name":"d","persistentVolumeClaim":{"claimName":"ufs-example"}}]}}'
kubectl wait --for=jsonpath='{.status.phase}'=Succeeded pod/ufs-reader --timeout=120s
test "$(kubectl logs ufs-reader)" = "from-writer"
kubectl delete pod ufs-reader --wait

# Dynamic provisioning: each PVC gets an isolated directory on the shared export.
kubectl apply -f deploy/storageclass.yaml
kubectl apply -f deploy/example-dynamic.yaml
kubectl wait --for=jsonpath='{.status.phase}'=Succeeded pod/dyn-a pod/dyn-b --timeout=180s
# each pod only sees its own `who` file (the listing is just `who`, not the other volume's data)
test "$(kubectl logs dyn-a)" = "who"
test "$(kubectl logs dyn-b)" = "who"
PVA=$(kubectl get pvc dyn-a -o jsonpath='{.spec.volumeName}')
PVB=$(kubectl get pvc dyn-b -o jsonpath='{.spec.volumeName}')
test "$(kubectl -n jetstream-ufs exec deploy/ufs -c ufs -- cat /export/$PVA/who)" = "only-in-a"
test "$(kubectl -n jetstream-ufs exec deploy/ufs -c ufs -- cat /export/$PVB/who)" = "only-in-b"

# Deleting the PVC deletes the directory.
kubectl delete pod dyn-a dyn-b --wait
kubectl delete pvc dyn-a dyn-b --wait
for i in $(seq 30); do
  kubectl -n jetstream-ufs exec deploy/ufs -c ufs -- test ! -e /export/$PVA && break
  sleep 2
done
kubectl -n jetstream-ufs exec deploy/ufs -c ufs -- test ! -e /export/$PVA
# The same, over iroh.
sed "s/REPLACE_WITH_NODE_ID/$IROH_NODE/" deploy/storageclass-iroh.yaml | kubectl apply -f -
kubectl apply -f deploy/example-iroh.yaml
kubectl wait --for=jsonpath='{.status.phase}'=Succeeded pod/iroh-a --timeout=180s
PVI=$(kubectl get pvc iroh-a -o jsonpath='{.spec.volumeName}')
test "$(kubectl -n jetstream-ufs exec deploy/ufs -c ufs -- cat /export/$PVI/who)" = "over-iroh"

echo "e2e OK"
