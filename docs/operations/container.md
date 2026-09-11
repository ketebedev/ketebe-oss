# Server container

Ketebe's main server has a production-oriented container build for standalone self-hosted use. Registry publishing is handled separately by the release workflow.

## Build locally

From the repository root:

```bash
docker build -t ketebe-server:local .
```

The Dockerfile uses Rust 1.98 only in the builder stage. The runtime image does not contain the Rust toolchain.

## Run

```bash
docker volume create ketebe-data

docker run --rm \
  --name ketebe-server \
  -p 7610:7610 \
  -p 7611:7611 \
  -v ketebe-data:/var/lib/ketebe \
  ketebe-server:local
```

The image sets these container defaults:

- `KETEBE_DATA_DIR=/var/lib/ketebe`
- `KETEBE_HTTP_ADDR=0.0.0.0:7610`
- `KETEBE_GRPC_ADDR=0.0.0.0:7611`

REST is exposed on port `7610`; gRPC is exposed on port `7611`.

## Health

The image includes a Docker-compatible health check against:

```text
GET http://127.0.0.1:7610/healthz
```

A healthy server returns a JSON response containing `"status":"ok"`.

External verification:

```bash
curl --fail http://127.0.0.1:7610/healthz
```

## Persistence

`/var/lib/ketebe` is the persistent state boundary for the standalone container. Mount a Docker/Podman volume at that path. Replacing the container while reusing the same volume preserves control-plane and data state.

The server bootstraps the real default Organization and default Project on a clean data directory; it does not bypass the Organization -> Project -> Collection domain model.

## Runtime identity

The final image runs as the non-root `ketebe` user with numeric UID/GID `10001`.

## Scope

This image packages the main Ketebe server only. Docker Compose, registry publishing, release archives, Kubernetes and Helm are handled by the later public-release issues.
