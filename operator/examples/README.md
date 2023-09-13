# Apibara Operator Examples

This folder contains example manifests that you can use to test your Apibara
Operator installation.

### Getting Started

If you're deploying integrations that use data from the hosted streams, you must
configure your API Key in `apikey.yaml`.

Change the value of the `production` key to your key, then deploy it with:

```sh
kubectl apply -f apikey.yaml
```

