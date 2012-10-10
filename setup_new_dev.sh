#!/bin/bash
set -e

BDIR=dev_backup
RDIR=dev

if test -d dev/dev1; then
  echo "move or remove existing nodes in dev/ first" >&2
  exit 1
fi

make devrel
for i in $(seq 1 4); do
  path=dev$i/etc/app.config
  cp $BDIR/$path $RDIR/$path
done

ulimit -n 4096

for i in $(seq 1 4); do
  $RDIR/dev$i/bin/riak start 
done

sleep 2

for i in $(seq 2 4); do
  $RDIR/dev$i/bin/riak-admin cluster join dev1@127.0.0.1
done

sleep 2

$RDIR/dev2/bin/riak-admin cluster plan
$RDIR/dev2/bin/riak-admin cluster commit

sleep 2

$RDIR/dev2/bin/search-cmd set-schema user $BDIR/user_schema

sleep 2

for i in $(seq 1 4); do
  $RDIR/dev$i/bin/search-cmd install user
done
