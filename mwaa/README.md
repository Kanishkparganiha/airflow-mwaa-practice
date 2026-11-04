# Local MWAA

Runs [AWS's MWAA image](https://github.com/aws/amazon-mwaa-docker-images) locally on Airflow 3.3.1, the latest version on production MWAA as of November 2026. No AWS account is needed.

**Status:** the config has been validated with `docker compose config`, but the image has not been built or run yet.

## Requirements

- Docker with Compose 2.24 or later
- About 8 GB of memory for Docker. Stop the plain Airflow stack in `../docker-compose.yml` first if memory is tight.
- Free ports: 8080 (UI), 5433 (metadata DB), 9324 and 9325 (local SQS)

## Usage

Run from the repo root:

```bash
./mwaa/mwaa.sh build    # first time only; AWS says builds can take 10-20 minutes
./mwaa/mwaa.sh up       # UI at http://localhost:8080
./mwaa/mwaa.sh status
./mwaa/mwaa.sh down
```

Run `./mwaa/mwaa.sh` with no arguments to see all commands.

## How it maps to production MWAA

| Local | Production MWAA |
|---|---|
| `../dags/` | `dags/` in your S3 bucket |
| `plugins/` | `plugins.zip` |
| `requirements/requirements.txt` | `requirements.txt` |
| `startup/startup.sh` | Startup script |
| ElasticMQ | Amazon SQS |
| Bundled Postgres | Managed metadata DB |

S3 sync, IAM, VPC networking and CloudWatch logging are not part of this setup.

## Notes

- `../dags/` is shared with the plain Airflow 2.9.1 stack. Those DAGs were written for Airflow 2 and have not been tested on 3.3.1, so some may need changes.
- This stack has its own Postgres and doesn't use the `adventureworks` container.
- `mwaa.sh` clones AWS's repo into `amazon-mwaa-docker-images/` at a pinned commit, and generates a local Fernet key in `.env`. Both are git-ignored.
