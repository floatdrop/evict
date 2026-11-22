#!/usr/bin/env bash
#
# Runs the throughput benchmarks on a virtual machine in Yandex Cloud, to see
# the caches on more CPUs than a laptop has, and on Linux on x86. It rents the
# machine, runs the benchmarks there COUNT times from start to end the way
# `make bench-throughput` does, copies the results back into OUT, and deletes
# the machine, also when something fails or the script is interrupted.
#
#   bench/yandex-cloud.sh
#   CORES=64 COUNT=3 bench/yandex-cloud.sh
#   CORES=16 BENCH='^BenchmarkGet$/cache=otter$' bench/yandex-cloud.sh
#
# It needs yc, the CLI of Yandex Cloud, set up with `yc init` for a folder with
# the quota for CORES vCPUs and for one more network. The machine gets a network
# of its own, deleted with it, so that the routes and neighbours of whatever
# else runs in the folder stay out of the way; SUBNET puts it in another one.
# The benchmarks are compiled here, from the working tree, so the machine needs
# nothing but SSH.
#
# A vCPU on these platforms is one of the two threads of a physical core, and
# the two threads of a core share its L1 and L2 caches. The benchmarks are
# therefore pinned to one thread of every core, so that a number of CPUs in the
# results is a number of cores: 32 vCPUs measure up to 16. SMT=1 uses them all.
#
# Should the script die without deleting the machine, the machine powers itself
# off after DEADLINE minutes. It then costs only its disk until it is deleted.

set -euo pipefail

CORES=${CORES:-32}                # vCPUs to rent, two to a physical core.
MEMORY=${MEMORY:-$CORES}          # GB.
PLATFORM=${PLATFORM:-standard-v3} # Intel Ice Lake.
ZONE=${ZONE:-ru-central1-a}
SUBNET=${SUBNET:-}                # A network of its own when empty.
IMAGE_FAMILY=${IMAGE_FAMILY:-ubuntu-2404-lts}
PREEMPTIBLE=${PREEMPTIBLE:-1}     # Cheaper, but the cloud may stop it at any time.
COUNT=${COUNT:-5}
BENCH=${BENCH:-.}                 # The benchmarks to run, as for go test -bench.
CPUS=${CPUS:-}                    # Powers of two up to the number of cores when empty.
SMT=${SMT:-0}
DEADLINE=${DEADLINE:-360}
OUT=${OUT:-bench/results/yandex-$PLATFORM-$CORES}

cd "$(dirname "$0")/.."

die() {
	echo "$(basename "$0"): $*" >&2
	exit 1
}

command -v yc > /dev/null || die "yc is not installed: see https://yandex.cloud/en/docs/cli/quickstart"
command -v jq > /dev/null || die "jq is not installed"
[[ -n $(yc config get folder-id 2> /dev/null) ]] || die "yc has no folder to work in: run yc init"

tmp=$(mktemp -d)
# The machine, and the network and subnet it gets, are all called name. Each
# variable below is set before the thing it stands for is created, so that the
# cleanup looks for it even when the script was stopped halfway.
name=evict-bench-$(date +%Y%m%d-%H%M%S)
instance=
network=
creating=

# remove deletes the thing called name that a yc command such as `compute
# instance` manages, if there is one, and says how to delete it by hand if it
# cannot. It fails if there is none.
remove() {
	local what=$1
	shift
	yc "$@" get --name "$name" > /dev/null 2>&1 || return 1
	echo "Deleting the $what $name"
	yc "$@" delete --name "$name" > /dev/null ||
		echo "Could not delete the $what $name: run yc $* delete --name $name" >&2
}

cleanup() {
	local code=$?
	if [[ -n $instance ]] && ! remove machine compute instance && [[ -n $creating ]]; then
		echo "Interrupted while $name was being created: see yc compute instance list" >&2
	fi
	if [[ -n $network ]]; then
		remove subnet vpc subnet || :
		remove network vpc network || :
	fi
	rm -rf "$tmp"
	exit "$code"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

# Every platform of Yandex Cloud is x86.
echo "Compiling the benchmarks for linux/amd64"
(cd bench && GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go test -c -o "$tmp/bench.test" .)
rev=$(git describe --always --dirty)

if [[ -z $SUBNET ]]; then
	echo "Creating the network $name"
	network=1
	yc vpc network create --name "$name" > /dev/null
	SUBNET=$(yc vpc subnet create --name "$name" --network-name "$name" --zone "$ZONE" \
		--range 10.128.0.0/24 --format json | jq -r .id)
fi
image=$(yc compute image get-latest-from-family "$IMAGE_FAMILY" --folder-id standard-images --format json | jq -r .id)

ssh-keygen -q -t ed25519 -N '' -C evict-bench -f "$tmp/key"
cat > "$tmp/user-data" << EOF
#cloud-config
users:
  - name: bench
    shell: /bin/bash
    ssh_authorized_keys:
      - $(cat "$tmp/key.pub")
runcmd:
  - shutdown -h +$DEADLINE
EOF

# What the machine runs, detached from the SSH session that starts it so that
# a connection that drops does not end the benchmarks. It leaves its exit code
# in a file called status.
{
	printf 'COUNT=%q BENCH=%q CPUS=%q SMT=%q REV=%q\n' "$COUNT" "$BENCH" "$CPUS" "$SMT" "$rev"
	cat << 'EOF'
set -euo pipefail
trap 'echo $? > status' EXIT
if [[ $SMT == 1 ]]; then
	pin=0-$(($(nproc --all) - 1))
else
	# The first of the threads of every core.
	pin=$(sed 's/[,-].*//' /sys/devices/system/cpu/cpu[0-9]*/topology/thread_siblings_list | sort -nu | paste -sd,)
fi
n=$(taskset -c "$pin" nproc)
if [[ -z $CPUS ]]; then
	CPUS=1
	for ((c = 2; c < n; c *= 2)); do CPUS+=,$c; done
	if ((n > 1)); then CPUS+=,$n; fi
fi
{
	echo "commit: $REV"
	echo "pinned to CPUs: $pin"
	echo "-bench: $BENCH"
	echo "-cpu: $CPUS"
	uname -a
	lscpu
} > machine.txt
for ((run = 1; run <= COUNT; run++)); do
	taskset -c "$pin" ./bench.test -test.run='^$' -test.bench="$BENCH" -test.cpu="$CPUS" -test.timeout=0 >> throughput.txt
done
EOF
} > "$tmp/run.sh"

args=(
	--name "$name" --zone "$ZONE" --platform "$PLATFORM"
	--cores "$CORES" --core-fraction 100 --memory "${MEMORY}GB"
	--create-boot-disk "image-id=$image,type=network-hdd,size=20GB,auto-delete=true"
	--network-interface "subnet-id=$SUBNET,nat-ip-version=ipv4"
	--metadata-from-file "user-data=$tmp/user-data"
	--format json
)
if [[ $PREEMPTIBLE == 1 ]]; then
	args+=(--preemptible)
fi
echo "Creating $name: $CORES vCPUs and ${MEMORY} GB on $PLATFORM in $ZONE"
instance=1 creating=1
yc compute instance create "${args[@]}" > "$tmp/instance.json"
creating=
ip=$(jq -r '.network_interfaces[0].primary_v4_address.one_to_one_nat.address' "$tmp/instance.json")

remote() {
	ssh -i "$tmp/key" -o IdentitiesOnly=yes -o BatchMode=yes -o LogLevel=ERROR \
		-o ConnectTimeout=10 -o ServerAliveInterval=15 -o ServerAliveCountMax=4 \
		-o StrictHostKeyChecking=accept-new -o UserKnownHostsFile="$tmp/known_hosts" \
		"bench@$ip" "$@"
}

echo "Waiting for $ip to accept SSH"
for ((i = 0; ; i++)); do
	if remote true 2> "$tmp/ssh.err"; then break; fi
	if ((i == 30)); then
		# What the machine printed while it booted tells whether it is up,
		# has an address and runs sshd, which SSH from here cannot.
		mkdir -p "$OUT"
		yc compute instance get-serial-port-output --name "$name" > "$OUT/serial.txt" 2>&1 || :
		die "$ip did not accept SSH in five minutes: $(tail -n 1 "$tmp/ssh.err"); see $OUT/serial.txt"
	fi
	sleep 10
done

remote 'cat > bench.test && chmod +x bench.test' < "$tmp/bench.test"
remote 'cat > run.sh && : > throughput.txt' < "$tmp/run.sh"
remote 'setsid nohup bash run.sh > run.log 2>&1 < /dev/null &'
echo "Running the benchmarks $COUNT times"

progress='printf "%s %s %s\n" "$(cat status 2> /dev/null || echo -)" "$(grep -c "^PASS" throughput.txt)" "$(grep -c "^Benchmark" throughput.txt)"'
start=$SECONDS
while :; do
	sleep 60
	# A connection that fails is tried again a minute later, unless the
	# machine is no longer running. One that cannot be asked either may be
	# the network here.
	if ! state=$(remote "$progress" 2> /dev/null); then
		vm=$(yc compute instance get --name "$name" --format json 2> /dev/null | jq -r .status) || vm=
		if [[ -n $vm && $vm != RUNNING ]]; then
			die "the machine is $vm: was it preempted?"
		fi
		continue
	fi
	read -r status runs results <<< "$state"
	echo "$(((SECONDS - start) / 60)) min: $runs of $COUNT runs done, $results results"
	[[ $status == - ]] || break
done

mkdir -p "$OUT"
remote 'tar -cf - throughput.txt machine.txt run.log' | tar -C "$OUT" -xf -
# What the benchmarks wrote to stderr, which is nothing when they pass.
[[ -s $OUT/run.log ]] || rm "$OUT/run.log"
[[ $status == 0 ]] || die "the benchmarks failed with status $status: see $OUT/run.log"
OUT=$(cd "$OUT" && pwd)
(cd bench && go run ./cmd/benchtable -charts "$OUT" < "$OUT/throughput.txt" > "$OUT/throughput.md")
echo "The results are in $OUT"
