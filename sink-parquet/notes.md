- if the batch data contains null values, the sink fails with:
````
thread 'main' panicked at 'called `Result::unwrap()` on an `Err` value: <Error: Encountered null only field. This error can be disabled by setting `allow_null_fields` to `true` in `TracingOptions`
```

- Errors for some reason:
````
thread 'main' panicked at 'called `Result::unwrap()` on an `Err` value: Stream(Status { code: Internal, message: "h2 protocol error: error reading a body from connection: stream error received: unspecific protocol error detected", source: Some(hyper::Error(Body, Error { kind: Reset(StreamId(1), PROTOCOL_ERROR, Remote) })) })', sink-parquet/src/bin.rs:40:46
```

- If the trait function has a default implementation of `Ok(())`, using it would require Sync + Send, see Sink imp
- Batch size 10
thread 'main' panicked at 'called `Result::unwrap()` on an `Err` value: Stream(Status { code: OutOfRange, message: "Error, message length too large: found 1707167 bytes, the limit is: 1000000 bytes", source: None })', sink-parquet/src/bin.rs:35:46
