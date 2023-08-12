# Apibara CLI installation script

This folder contains the installation script for the Apibara CLI.

## Testing

Build the `Dockerfile.test` image with the following command:

```sh
docker build --no-cache -t cli-test -f Dockerfile.test .
```

If the build succeeds, the installation script is working.
