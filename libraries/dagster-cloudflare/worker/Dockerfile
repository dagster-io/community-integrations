# The run container is your Dagster code image. The launcher sets the entrypoint
# to the `dagster api execute_run` command, so the image needs nothing extra.
# Cloudflare Containers run linux/amd64 images.
FROM --platform=linux/amd64 my-registry/my-dagster-code:latest
