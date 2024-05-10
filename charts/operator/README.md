# Apibara Operator

The Apibara Operator is a Kubernetes operator to deploy indexers.

## Usage

The chart is published to an OCI Helm repository.

Inspect the chart with the `helm show` command:

```
$ helm show readme oci://quay.io/apibara-charts/operator
---
Pulled: quay.io/apibara-charts/operator:0.1.0
Digest: sha256:a248767bcfbb2973b616052dcc38b791f1b6ff13f2db40b61951183f85c0729e
# Apibara Operator

The Apibara Operator is a Kubernetes operator to deploy indexers.
```

Install the chart with `helm install`.

```
$ helm install capy oci://quay.io/apibara-charts/operator
---
Pulled: quay.io/apibara-charts/operator:0.1.0
Digest: sha256:a248767bcfbb2973b616052dcc38b791f1b6ff13f2db40b61951183f85c0729e
NAME: capy
LAST DEPLOYED: Fri May 10 21:00:30 2024
NAMESPACE: default
STATUS: deployed
REVISION: 1
TEST SUITE: None
NOTES:
capy-operator has been installed. Check its status by running:

    kubectl --namespace default get pods

```

Customize the release using `values.yaml` as usual.
