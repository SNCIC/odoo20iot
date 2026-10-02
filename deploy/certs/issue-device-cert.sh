#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat >&2 <<'EOF'
用法:
  issue-device-cert.sh \
    --device-key DEVICE_KEY \
    --ca-cert CA_CERT \
    --ca-key CA_KEY \
    --out-dir OUTPUT_DIR

输出:
  OUTPUT_DIR/DEVICE_KEY.key
  OUTPUT_DIR/DEVICE_KEY.crt
  OUTPUT_DIR/DEVICE_KEY.chain.crt
EOF
  exit 2
}

device_key=''
ca_cert=''
ca_key=''
out_dir=''

while [[ $# -gt 0 ]]; do
  case "$1" in
    --device-key) device_key=${2:-}; shift 2 ;;
    --ca-cert) ca_cert=${2:-}; shift 2 ;;
    --ca-key) ca_key=${2:-}; shift 2 ;;
    --out-dir) out_dir=${2:-}; shift 2 ;;
    -h|--help) usage ;;
    *) echo "未知参数: $1" >&2; usage ;;
  esac
done

[[ -n "$device_key" && -n "$ca_cert" && -n "$ca_key" && -n "$out_dir" ]] || usage
[[ "$device_key" =~ ^[A-Za-z0-9._-]+$ ]] || { echo 'device-key 含有不允许的字符' >&2; exit 2; }
[[ -r "$ca_cert" ]] || { echo "无法读取 CA 证书: $ca_cert" >&2; exit 1; }
[[ -r "$ca_key" ]] || { echo "无法读取 CA 私钥: $ca_key" >&2; exit 1; }

mkdir -p "$out_dir"
chmod 700 "$out_dir"

tmp_dir=$(mktemp -d "${out_dir}/.${device_key}.XXXXXX")
cleanup() { rm -rf "$tmp_dir"; }
trap cleanup EXIT

key="$tmp_dir/$device_key.key"
csr="$tmp_dir/$device_key.csr"
crt="$tmp_dir/$device_key.crt"
chain="$tmp_dir/$device_key.chain.crt"
serial="$tmp_dir/ca.srl"

openssl genpkey -algorithm ED25519 -out "$key" >/dev/null 2>&1
openssl req -new -key "$key" -out "$csr" -subj "/CN=$device_key" >/dev/null 2>&1
openssl x509 -req -in "$csr" -CA "$ca_cert" -CAkey "$ca_key" \
  -CAcreateserial -CAserial "$serial" -days 365 -sha256 \
  -out "$crt" >/dev/null 2>&1
cat "$crt" "$ca_cert" > "$chain"

openssl verify -CAfile "$ca_cert" "$crt" >/dev/null
[[ "$(openssl x509 -in "$crt" -noout -subject)" == *"CN = $device_key"* ]] || {
  echo '签发结果的 CN 校验失败' >&2
  exit 1
}

install -m 600 "$key" "$out_dir/$device_key.key"
install -m 644 "$crt" "$out_dir/$device_key.crt"
install -m 644 "$chain" "$out_dir/$device_key.chain.crt"
echo "已签发设备证书: $out_dir/$device_key.crt"
