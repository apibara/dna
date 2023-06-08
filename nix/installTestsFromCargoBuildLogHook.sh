# This is a modified version of the installFromCargoBuildLog.sh script
# that installs all test artifacts from the cargo build log.
function installTestsFromCargoBuildLog() (
  local dest=${1:-${out}}
  local log=${2:-${cargoBuildLog:?not defined}}

  if ! [ -f "${log}" ]; then
    echo unable to install, cargo build log does not exist at: ${log}
    false
  fi

  echo searching for tests to install from cargo build log at ${log}

  local logs
  logs=$(jq -R 'fromjson?' <"${log}")

  local select_test='select(.reason == "compiler-artifact" and .profile.test == true)'
  local select_bins="${select_test} | .executable | select(.!= null)"

  function installArtifacts() {
    local loc=${1?:missing}
    mkdir -p "${loc}"

    while IFS= read -r to_install; do
      echo installing ${to_install}
      cp "${to_install}" "${loc}"
    done

    rmdir --ignore-fail-on-non-empty "${loc}"
  }

  echo "${logs}" | jq -r "${select_bins}" | installArtifacts "${dest}/bin"

  echo searching for tests complete
)
