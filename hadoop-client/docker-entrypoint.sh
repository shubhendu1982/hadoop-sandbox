#! /bin/bash

set -e

get_config_option() {
	option="$1"

	[ -f /etc/ssh/sshd_config ] || return

	# TODO: actually only one '=' allowed after option
	perl -lne '
		s/[[:space:]]+/ /g; s/[[:space:]]+$//;
		print if s/^[[:space:]]*'"$option"'[[:space:]=]+//i' \
	   /etc/ssh/sshd_config
}


host_keys_required() {
	hostkeys="$(get_config_option HostKey)"
	if [ "$hostkeys" ]; then
		echo "$hostkeys"
	else
		# No HostKey directives at all, so the server picks some
		# defaults.
		echo /etc/ssh/ssh_host_rsa_key
		echo /etc/ssh/ssh_host_ecdsa_key
		echo /etc/ssh/ssh_host_ed25519_key
	fi
}


create_key() {
	msg="$1"
	shift
	hostkeys="$1"
	shift
	file="$1"
	shift

	if echo "$hostkeys" | grep -x "$file" >/dev/null && \
	   [ ! -f "$file" ] ; then
		echo -n $msg
		ssh-keygen -q -f "$file" -N '' "$@"
		echo
		if which restorecon >/dev/null 2>&1; then
			restorecon "$file" "$file.pub"
		fi
		ssh-keygen -l -f "$file.pub"
	fi
}


create_keys() {
	hostkeys="$(host_keys_required)"

	create_key "Creating SSH2 RSA key; this may take some time ..." \
		"$hostkeys" /etc/ssh/ssh_host_rsa_key -t rsa
	create_key "Creating SSH2 DSA key; this may take some time ..." \
		"$hostkeys" /etc/ssh/ssh_host_dsa_key -t dsa
	create_key "Creating SSH2 ECDSA key; this may take some time ..." \
		"$hostkeys" /etc/ssh/ssh_host_ecdsa_key -t ecdsa
	create_key "Creating SSH2 ED25519 key; this may take some time ..." \
		"$hostkeys" /etc/ssh/ssh_host_ed25519_key -t ed25519
}

create_keys

gosu hdfs "${HADOOP_HOME}/bin/hdfs" dfs -mkdir -p /user/sandbox
gosu hdfs "${HADOOP_HOME}/bin/hdfs" dfs -chown sandbox:sandbox /user/sandbox

exec "$@"
