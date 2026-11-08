#!/bin/bash
set -euo pipefail

# 此脚本作为部署公钥的 forced command，只接受上传与发布两个操作。
if [[ ${SSH_ORIGINAL_COMMAND:-} =~ ^upload(-gzip)?\ ([a-f0-9]{40})$ ]]; then
    compression=${BASH_REMATCH[1]}
    sha=${BASH_REMATCH[2]}
    staging=/var/lib/golitube-deploy/uploads
    temporary=$(mktemp "$staging/.upload.XXXXXX")
    trap 'rm -f "$temporary"' EXIT
    if [[ $compression == -gzip ]]; then
        gzip -dc | head -c 104857601 > "$temporary"
    else
        head -c 104857601 > "$temporary"
    fi
    test "$(stat -c %s "$temporary")" -le 104857600
    test -s "$temporary"
    chmod 600 "$temporary"
    mv "$temporary" "$staging/$sha"
    echo "Uploaded $sha"
elif [[ ${SSH_ORIGINAL_COMMAND:-} =~ ^deploy\ ([a-f0-9]{40})$ ]]; then
    exec sudo -n /usr/local/sbin/golitube-deploy "${BASH_REMATCH[1]}"
else
    echo "Only upload <sha>, upload-gzip <sha> and deploy <sha> are accepted" >&2
    exit 1
fi
