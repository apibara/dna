# Apibara 🤝 Webhooks

_Send the result of transforming a batch to a webhook._

**Use cases**

- Serverless indexers using Cloudflare Functions, AWS Lambda, or any serverless
  function.
- Start a background job on Inngest or other job queues.
- Push messages into a message queue like Kafka or AWS SNS.

**Usage**

You must set the `WEBHOOK_TARGET_URL` environment variable to the target url.
Alternatively, you can update the scripts with your target url in the
`sinkOptions` field.
