#!/usr/bin/env bash
# Runs AWS's MWAA image locally, next to the plain Airflow setup in ../docker-compose.yml.
set -euo pipefail

MWAA_VERSION="3.3.1"
AWS_REPO_URL="https://github.com/aws/amazon-mwaa-docker-images.git"
AWS_REPO_COMMIT="99872d2c4c7c0934332bb2355e636824dbcd6a48"

MWAA_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
AWS_REPO="$MWAA_DIR/amazon-mwaa-docker-images"
VERSION_DIR="$AWS_REPO/images/airflow/$MWAA_VERSION"
ENV_FILE="$MWAA_DIR/.env"

export PRACTICE_ROOT="$(dirname "$MWAA_DIR")"

# Dummy credentials are enough offline: ElasticMQ (local SQS) doesn't validate them,
# but boto3 needs non-empty values to sign requests.
export AWS_ACCESS_KEY_ID="local"
export AWS_SECRET_ACCESS_KEY="local"
export AWS_SESSION_TOKEN=""
export REGION="us-west-2"

export MWAA__CORE__REQUIREMENTS_PATH="/usr/local/airflow/requirements/requirements.txt"
export MWAA__CORE__STARTUP_SCRIPT_PATH="/usr/local/airflow/startup/startup.sh"
export MWAA__CORE__KMS_KEY_ARN=""
export MWAA__CORE__TASK_MONITORING_ENABLED="false"
export MWAA__CORE__TERMINATE_IF_IDLE="false"
export MWAA__CORE__MWAA_SIGNAL_HANDLING_ENABLED="false"
export MWAA__CORE__API_SERVER_URL="http://mwaa-${MWAA_VERSION//./}-webserver:8080"
export MWAA__HEALTH_MONITORING_ENABLE_REVAMPED_HEALTHCHECK=""

# Logs stay inside the containers rather than going to CloudWatch.
for component in DAGPROCESSOR SCHEDULER TASK TRIGGERER WEBSERVER WORKER; do
  export "MWAA__LOGGING__AIRFLOW_${component}_LOGS_ENABLED=false"
  export "MWAA__LOGGING__AIRFLOW_${component}_LOG_GROUP_ARN="
  export "MWAA__LOGGING__AIRFLOW_${component}_LOG_LEVEL=INFO"
done

ensure_repo() {
  if [[ ! -d "$AWS_REPO/.git" ]]; then
    git init -q "$AWS_REPO"
    git -C "$AWS_REPO" remote add origin "$AWS_REPO_URL"
    git -C "$AWS_REPO" fetch -q --depth 1 origin "$AWS_REPO_COMMIT"
    git -C "$AWS_REPO" checkout -q FETCH_HEAD
  fi
}

# MWAA expects the key wrapped in JSON. The value is the same format Fernet.generate_key() returns.
ensure_env() {
  if [[ ! -f "$ENV_FILE" ]]; then
    local key
    key="$(head -c 32 /dev/urandom | base64 | tr '+/' '-_')"
    printf "FERNET_KEY='{\"FernetKey\": \"%s\"}'\n" "$key" > "$ENV_FILE"
    chmod 600 "$ENV_FILE"
  fi
  set -a
  # shellcheck source=/dev/null
  source "$ENV_FILE"
  set +a
}

compose() {
  docker compose -p "mwaa-${MWAA_VERSION//./}" \
    --project-directory "$VERSION_DIR" \
    -f "$VERSION_DIR/docker-compose.yaml" \
    -f "$MWAA_DIR/docker-compose.override.yml" \
    "$@"
}

# Builds only the two images docker-compose.yaml needs. AWS's build.sh also builds 5 more variants.
build() {
  ensure_repo
  docker build -f "$VERSION_DIR/Dockerfiles/Dockerfile.base" \
    -t "localhost/amazon-mwaa-docker-images/airflow:${MWAA_VERSION}-base" "$VERSION_DIR"
  docker build -f "$VERSION_DIR/Dockerfiles/Dockerfile-dev" \
    -t "amazon-mwaa-docker-images/airflow:${MWAA_VERSION}-dev" "$VERSION_DIR"
}

usage() {
  cat <<EOF
Usage: ./mwaa/mwaa.sh <command>

  build            Clone AWS's repo if needed and build the images
  up               Start the stack (UI at http://localhost:8080)
  down             Stop the stack, keeping the metadata DB
  reset            Stop the stack and delete the metadata DB
  status           Show containers
  config           Print the merged compose config without starting anything
  logs [service]   Follow logs: webserver, scheduler, worker, migratedb
  shell [service]  Open a shell in a container (default: scheduler)
  airflow <args>   Run the Airflow CLI, e.g. ./mwaa/mwaa.sh airflow dags list
EOF
}

command="${1:-}"
[[ $# -gt 0 ]] && shift

case "$command" in
  build) build ;;
  up) ensure_repo; ensure_env; compose up -d; echo "MWAA $MWAA_VERSION: http://localhost:8080" ;;
  down) ensure_env; compose down ;;
  reset) ensure_env; compose down -v ;;
  status) ensure_env; compose ps ;;
  config) ensure_repo; ensure_env; compose config ;;
  logs) ensure_env; compose logs -f "$@" ;;
  shell) ensure_env; compose exec "${1:-scheduler}" bash ;;
  airflow) ensure_env; compose exec scheduler airflow "$@" ;;
  *) usage; [[ -z "$command" ]] || exit 1 ;;
esac
