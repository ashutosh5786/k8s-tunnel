#!/bin/sh
# Runs inside a Linux container with the cross-compiled binaries mounted at
# /work. It runs every package's tests, then exercises the real ashctl
# background connection against a real tunnel-server, with no cluster.
#
# Build the inputs on the host first (see linux-check.ps1 or the README),
# then:
#   docker run --rm -v <repo>/dist/linux:/work --entrypoint sh alpine/helm:3.16.4 /work/linux-check.sh
set -u
cd /work
fail=0

echo "===== tests ====="
for t in tests/*.test; do
  if out=$("./$t" -test.count=1 2>&1); then
    echo "ok    $t"
  else
    echo "FAIL  $t"
    echo "$out" | tail -40
    fail=1
  fi
done

echo
echo "===== ashctl in the background ====="
export ASHCTL_HOME=/tmp/ashctl-home
cp /etc/hosts /tmp/hosts.before

# A stand-in "cluster service" that answers every connection with one line.
( while true; do echo "hello from the cluster side" | nc -l -p 18001 >/dev/null 2>&1 || sleep 1; done ) &
./tunnel-server -publish=false -listen 127.0.0.1:7000 >/tmp/server.log 2>&1 &
sleep 1

step() { echo; echo "--- $1"; }
check() { # check <description> <command...>
  desc=$1; shift
  if "$@" >/tmp/check.out 2>&1; then echo "ok    $desc"; else echo "FAIL  $desc"; cat /tmp/check.out; fail=1; fi
}

step "status before connecting"
./ashctl status

step "connect (must return by itself)"
start=$(date +%s)
./ashctl connect --server 127.0.0.1:7000 --user ash --forward 18003:127.0.0.1:18001
rc=$?
took=$(( $(date +%s) - start ))
check "connect returned success" test "$rc" -eq 0
check "connect returned promptly (${took}s)" test "$took" -lt 30

step "status while connected"
./ashctl status
check "status reports connected" sh -c './ashctl status | grep -q "^Connected"'

step "traffic through the background process"
reply=$(nc -w 3 127.0.0.1 18003 </dev/null 2>/dev/null)
echo "reply: $reply"
check "forward works after connect returned" test "$reply" = "hello from the cluster side"

step "expose on the running connection"
# A stand-in "local service" for the cluster to reach.
( while true; do echo "hello from the laptop side" | nc -l -p 18002 >/dev/null 2>&1 || sleep 1; done ) &
sleep 1
./ashctl expose b-ash:8080:18002/http | tee /tmp/expose.out
port=$(sed -n 's/.*server port \([0-9]*\) .*/\1/p' /tmp/expose.out | head -1)
check "expose printed where the cluster reaches it" test -n "$port"
reply=$(nc -w 3 127.0.0.1 "$port" </dev/null 2>/dev/null)
echo "reply: $reply"
check "cluster-side port reaches the local service" test "$reply" = "hello from the laptop side"
./ashctl status
check "status lists the exposure" sh -c './ashctl status | grep -q "localhost:18002"'

step "unexpose"
./ashctl unexpose b-ash
check "exposed port is closed again" sh -c "! nc -w 2 127.0.0.1 $port </dev/null"
check "status no longer lists it" sh -c './ashctl status | grep -q "Nothing exposed"'
check "unexposing twice is an error" sh -c '! ./ashctl unexpose b-ash'

step "second connect is refused"
./ashctl connect --server 127.0.0.1:7000 --user ash
check "second connect failed" sh -c '! ./ashctl connect --server 127.0.0.1:7000 --user ash'

step "disconnect"
./ashctl disconnect
check "status reports not connected" sh -c './ashctl status | grep -q "Not connected"'
check "forward port is closed" sh -c '! nc -w 2 127.0.0.1 18003 </dev/null'
check "hosts file is unchanged" cmp -s /etc/hosts /tmp/hosts.before
check "server saw the session end" grep -q "session ended" /tmp/server.log

step "background log"
./ashctl logs

echo
if [ "$fail" -eq 0 ]; then echo "ALL CHECKS PASSED"; else echo "SOME CHECKS FAILED"; fi
exit "$fail"
