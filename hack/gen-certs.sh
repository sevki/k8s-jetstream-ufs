#!/usr/bin/env bash
# Generates a CA plus server and client certificates for QUIC mTLS into <outdir>.
# usage: gen-certs.sh <outdir> [server-dns-name]
set -euo pipefail
out=${1:?outdir}
san=${2:-ufs.jetstream-ufs.svc.cluster.local}
mkdir -p "$out"
cd "$out"

openssl req -x509 -newkey rsa:2048 -nodes -keyout ca.key -out ca.crt -subj /CN=ufs-ca -days 30 \
  -addext basicConstraints=critical,CA:TRUE 2>/dev/null

issue() { # name usage san
  openssl req -newkey rsa:2048 -nodes -keyout "$1.key" -out "$1.csr" -subj "/CN=$1" 2>/dev/null
  printf 'subjectAltName=%s\nextendedKeyUsage=%s\n' "$3" "$2" > "$1.ext"
  openssl x509 -req -in "$1.csr" -CA ca.crt -CAkey ca.key -CAcreateserial -out "$1.crt" -days 30 \
    -extfile "$1.ext" 2>/dev/null
}
issue server serverAuth "DNS:$san"
issue client clientAuth "DNS:ufs-csi-client"
