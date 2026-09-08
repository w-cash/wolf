#!/usr/bin/env bash

# Entrypoint for running the Wcash node in Docker.
#
# This script handles privilege dropping and launches zebrad or tests.
# Configuration is managed by config-rs using defaults, optional TOML, and
# environment variables prefixed with WCASH_.

set -eo pipefail

# Cache directories that the entrypoint must prepare before dropping privileges.
# In environment-only mode, export the defaults for config-rs. When a TOML file
# is present, avoid overriding its paths; caller-provided WCASH_* values remain
# exported and still take precedence over the file.
: "${WCASH_STATE__CACHE_DIR:=${HOME}/.cache/wcash}"
: "${WCASH_NETWORK__CACHE_DIR:=${HOME}/.cache/wcash}"
: "${WCASH_RPC__COOKIE_DIR:=${HOME}/.cache/wcash}"

WCASH_CONFIG_PATH="${CONFIG_FILE_PATH:-${HOME}/.config/wcash.toml}"
if [[ ! -f ${WCASH_CONFIG_PATH} ]]; then
  export WCASH_STATE__CACHE_DIR
  export WCASH_NETWORK__CACHE_DIR
  export WCASH_RPC__COOKIE_DIR
fi

# Leave zcashd-compat disabled unless the container runtime explicitly opts in.
# Compat images can set ZCASHD_COMPAT_ENABLED=true to use a vendored
# /usr/local/bin/zcashd, while still allowing WCASH_ZCASHD_COMPAT__* overrides.
case "${ZCASHD_COMPAT_ENABLED:-}" in
true | TRUE | 1 | yes | YES | on | ON)
  export WCASH_ZCASHD_COMPAT__ENABLED="${WCASH_ZCASHD_COMPAT__ENABLED:-true}"
  if [[ -x /usr/local/bin/zcashd ]]; then
    export WCASH_ZCASHD_COMPAT__MANAGE_ZCASHD="${WCASH_ZCASHD_COMPAT__MANAGE_ZCASHD:-true}"
    export WCASH_ZCASHD_COMPAT__ZCASHD_SOURCE="${WCASH_ZCASHD_COMPAT__ZCASHD_SOURCE:-path}"
    export WCASH_ZCASHD_COMPAT__ZCASHD_PATH="${WCASH_ZCASHD_COMPAT__ZCASHD_PATH:-/usr/local/bin/zcashd}"
  fi
  ;;
false | FALSE | 0 | no | NO | off | OFF | "") ;;
*)
  echo "ZCASHD_COMPAT_ENABLED must be true or false" >&2
  exit 1
  ;;
esac

# Use setpriv to drop privileges and execute the given command as the specified UID:GID
exec_as_user() {
  user=$(id -u)
  if [[ ${user} == '0' ]]; then
    exec setpriv --reuid="${UID}" --regid="${GID}" --init-groups "$@"
  else
    exec "$@"
  fi
}

# Helper function
exit_error() {
  echo "$1" >&2
  exit 1
}

# Creates a directory if it doesn't exist and sets ownership to specified UID:GID.
create_owned_directory() {
  local dir="$1"
  # Skip if directory is empty
  [[ -z ${dir} ]] && return

  # Create directory with parents
  mkdir -p "${dir}" || exit_error "Failed to create directory: ${dir}"

  # Set ownership for the created directory
  chown -R "${UID}:${GID}" "${dir}" || exit_error "Failed to secure directory: ${dir}"

  # Set ownership for parent directory (but not if it's root or home)
  local parent_dir
  parent_dir="$(dirname "${dir}")"
  if [[ "${parent_dir}" != "/" && "${parent_dir}" != "${HOME}" ]]; then
    chown "${UID}:${GID}" "${parent_dir}"
  fi
}

# Prepares a zcashd datadir mount without a recursive chown.
#
# The datadir can be a large pre-synced tree (blocks/chainstate), so chowning
# it recursively on every start would be slow and would re-own files a host
# zcashd may still use. Only the top-level directory is chowned so zcashd can
# create its files, and a failure (e.g. a read-only inspection mount) warns
# instead of aborting the container — zcashd surfaces a real error if it truly
# cannot write.
create_owned_zcashd_datadir() {
  local dir="$1"
  [[ -z ${dir} ]] && return

  mkdir -p "${dir}" || exit_error "Failed to create zcashd datadir: ${dir}"
  chown "${UID}:${GID}" "${dir}" 2>/dev/null ||
    echo "WARNING: could not chown zcashd datadir ${dir}; relying on existing permissions" >&2
}

# Create and own cache and config directories based on WCASH_* environment variables.
[[ -n ${WCASH_STATE__CACHE_DIR} ]] && create_owned_directory "${WCASH_STATE__CACHE_DIR}"
[[ -n ${WCASH_NETWORK__CACHE_DIR} ]] && create_owned_directory "${WCASH_NETWORK__CACHE_DIR}"
[[ -n ${WCASH_RPC__COOKIE_DIR} ]] && create_owned_directory "${WCASH_RPC__COOKIE_DIR}"
[[ -n ${WCASH_ZCASHD_COMPAT__ZCASHD_DATADIR:-} ]] && create_owned_zcashd_datadir "${WCASH_ZCASHD_COMPAT__ZCASHD_DATADIR}"
[[ -n ${WCASH_TRACING__LOG_FILE:-} ]] && create_owned_directory "$(dirname "${WCASH_TRACING__LOG_FILE}")"

# --- Optional config file support ---
# If provided, pass a config file path through to zebrad via CONFIG_FILE_PATH.

# If the user provided a config file path we pass it to zebrad.
CONFIG_ARGS=()
if [[ -n ${CONFIG_FILE_PATH} && -f ${CONFIG_FILE_PATH} ]]; then
    echo "INFO: Using config file at ${CONFIG_FILE_PATH}"
    CONFIG_ARGS=(--config "${CONFIG_FILE_PATH}")
fi

# Main Script Logic
# - If "$1" is "--", "-", or "zebrad", run `zebrad` with the remaining params.
# - If "$1" is "test", handle test execution
# - Otherwise run "$@" directly.
case "$1" in
--* | -* | zebrad)
  shift
  exec_as_user zebrad "${CONFIG_ARGS[@]}" "$@"
  ;;
test)
  shift
  if [[ "$1" == "zebrad" ]]; then
    shift
    exec_as_user zebrad "${CONFIG_ARGS[@]}" "$@"
  elif [[ -n "${NEXTEST_PROFILE}" ]]; then
    if [[ "${NEXTEST_PROFILE}" =~ ^ci-(stateful|e2e)$ && -z "${NEXTEST_FILTER}" ]]; then
      exit_error "NEXTEST_FILTER is required when NEXTEST_PROFILE=${NEXTEST_PROFILE}"
    fi

    FILTER_ARGS=()
    if [[ -n "${NEXTEST_FILTER}" ]]; then
      FILTER_ARGS=(--filter-expr "${NEXTEST_FILTER}")
    fi
    echo "Running tests with profile=${NEXTEST_PROFILE} filter=${NEXTEST_FILTER:-all}"
    exec_as_user cargo nextest run --profile "${NEXTEST_PROFILE}" --locked --release --features "${FEATURES}" --run-ignored=all --hide-progress-bar "${FILTER_ARGS[@]}"
  else
    exec_as_user "$@"
  fi
  ;;
*)
  exec_as_user "$@"
  ;;
esac
