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
  kubectl -n jetstream-ufs logs deploy/ufs --tail=100 || true
}
trap 'dump' ERR

docker build -t "$IMAGE" .
kind create cluster --name "$CLUSTER" --wait 120s
kind load docker-image "$IMAGE" --name "$CLUSTER"

kubectl apply -f deploy/server.yaml
kubectl -n jetstream-ufs rollout status deploy/ufs --timeout=120s
kubectl -n jetstream-ufs exec deploy/ufs -- sh -c 'echo seed-data > /export/seed.txt'

kubectl apply -f deploy/csi-driver.yaml
kubectl -n kube-system rollout status ds/ufs-csi-node --timeout=180s

kubectl apply -f deploy/example.yaml
kubectl wait --for=jsonpath='{.status.phase}'=Succeeded pod/ufs-writer --timeout=180s

# The pod read the file seeded on the server...
test "$(kubectl logs ufs-writer)" = "seed-data"
# ...and what it wrote landed in the server's export directory.
test "$(kubectl -n jetstream-ufs exec deploy/ufs -- cat /export/written.txt)" = "from-writer"

# A second pod on the same volume sees the data (ReadWriteMany) and unmount works on delete.
kubectl delete pod ufs-writer --wait
kubectl run ufs-reader --image=busybox:1.36 --restart=Never --overrides='{
  "spec":{"containers":[{"name":"r","image":"busybox:1.36","command":["cat","/data/written.txt"],
  "volumeMounts":[{"name":"d","mountPath":"/data"}]}],
  "volumes":[{"name":"d","persistentVolumeClaim":{"claimName":"ufs-example"}}]}}'
kubectl wait --for=jsonpath='{.status.phase}'=Succeeded pod/ufs-reader --timeout=120s
test "$(kubectl logs ufs-reader)" = "from-writer"
echo "e2e OK"
