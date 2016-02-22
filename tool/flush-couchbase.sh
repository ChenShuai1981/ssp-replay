#!/usr/bin/env bash
/Applications/Couchbase\ Server.app/Contents/Resources/couchbase-core/bin/couchbase-cli bucket-flush --force -c localhost:8091 -u Administrator -p 123456 --bucket=ssp_dedup_key --enable-flush=1
/Applications/Couchbase\ Server.app/Contents/Resources/couchbase-core/bin/couchbase-cli bucket-flush --force -c localhost:8091 -u Administrator -p 123456 --bucket=ssp_dedup_offset --enable-flush=1
/Applications/Couchbase\ Server.app/Contents/Resources/couchbase-core/bin/couchbase-cli bucket-flush --force -c localhost:8091 -u Administrator -p 123456 --bucket=ssp_indexing_offset --enable-flush=1
