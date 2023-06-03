- if the batch data contains null values, the sink fails with:
````
thread 'main' panicked at 'called `Result::unwrap()` on an `Err` value: <Error: Encountered null only field. This error can be disabled by setting `allow_null_fields` to `true` in `TracingOptions`
Backtrace:
   0: std::backtrace_rs::backtrace::libunwind::trace
             at /rustc/84c898d65adf2f39a5a98507f1fe0ce10a2b8dbc/library/std/src/../../backtrace/src/backtrace/libunwind.rs:93:5
   1: std::backtrace_rs::backtrace::trace_unsynchronized
             at /rustc/84c898d65adf2f39a5a98507f1fe0ce10a2b8dbc/library/std/src/../../backtrace/src/backtrace/mod.rs:66:5
   2: std::backtrace::Backtrace::create
             at /rustc/84c898d65adf2f39a5a98507f1fe0ce10a2b8dbc/library/std/src/backtrace.rs:332:13
   3: serde_arrow::internal::error::Error::custom
             at /Users/mac/.cargo/registry/src/github.com-1ecc6299db9ec823/serde_arrow-0.7.1/src/internal/error.rs:26:24
   4: serde_arrow::internal::schema::UnknownTracer::to_field
             at /Users/mac/.cargo/registry/src/github.com-1ecc6299db9ec823/serde_arrow-0.7.1/src/internal/schema.rs:740:13
   5: serde_arrow::internal::schema::Tracer::to_field
             at /Users/mac/.cargo/registry/src/github.com-1ecc6299db9ec823/serde_arrow-0.7.1/src/internal/schema.rs:564:27
   6: serde_arrow::internal::schema::StructTracer::to_field
             at /Users/mac/.cargo/registry/src/github.com-1ecc6299db9ec823/serde_arrow-0.7.1/src/internal/schema.rs:807:33
   7: serde_arrow::internal::schema::Tracer::to_field
             at /Users/mac/.cargo/registry/src/github.com-1ecc6299db9ec823/serde_arrow-0.7.1/src/internal/schema.rs:570:26
   8: serde_arrow::internal::serialize_into_fields
             at /Users/mac/.cargo/registry/src/github.com-1ecc6299db9ec823/serde_arrow-0.7.1/src/internal/mod.rs:73:16
   9: serde_arrow::arrow2::serialize_into_fields
             at /Users/mac/.cargo/registry/src/github.com-1ecc6299db9ec823/serde_arrow-0.7.1/src/arrow2/mod.rs:80:5
  10: <apibara_sink_parquet::ParquetSink as apibara_sink_common::connector::Sink>::handle_data::{{closure}}
             at ./sink-parquet/src/lib.rs:94:22
  11: <core::pin::Pin<P> as core::future::future::Future>::poll
             at /rustc/84c898d65adf2f39a5a98507f1fe0ce10a2b8dbc/library/core/src/future/future.rs:125:9
  12: apibara_sink_common::connector::SinkConnector<F,B>::handle_message::{{closure}}
             at ./sink-common/src/connector.rs:250:25
  13: apibara_sink_common::connector::SinkConnector<F,B>::consume_stream::{{closure}}
             at ./sink-common/src/connector.rs:190:102
  14: apibara_sink_parquet::main::{{closure}}
             at ./sink-parquet/src/bin.rs:38:39
  15: tokio::runtime::park::CachedParkThread::block_on::{{closure}}
             at /Users/mac/.cargo/registry/src/github.com-1ecc6299db9ec823/tokio-1.28.2/src/runtime/park.rs:283:63
  16: tokio::runtime::coop::with_budget
             at /Users/mac/.cargo/registry/src/github.com-1ecc6299db9ec823/tokio-1.28.2/src/runtime/coop.rs:107:5
  17: tokio::runtime::coop::budget
             at /Users/mac/.cargo/registry/src/github.com-1ecc6299db9ec823/tokio-1.28.2/src/runtime/coop.rs:73:5
  18: tokio::runtime::park::CachedParkThread::block_on
             at /Users/mac/.cargo/registry/src/github.com-1ecc6299db9ec823/tokio-1.28.2/src/runtime/park.rs:283:31
  19: tokio::runtime::context::BlockingRegionGuard::block_on
             at /Users/mac/.cargo/registry/src/github.com-1ecc6299db9ec823/tokio-1.28.2/src/runtime/context.rs:315:13
  20: tokio::runtime::scheduler::multi_thread::MultiThread::block_on
             at /Users/mac/.cargo/registry/src/github.com-1ecc6299db9ec823/tokio-1.28.2/src/runtime/scheduler/multi_thread/mod.rs:66:9
  21: tokio::runtime::runtime::Runtime::block_on
             at /Users/mac/.cargo/registry/src/github.com-1ecc6299db9ec823/tokio-1.28.2/src/runtime/runtime.rs:304:45
  22: apibara_sink_parquet::main
             at ./sink-parquet/src/bin.rs:40:5
  23: core::ops::function::FnOnce::call_once
             at /rustc/84c898d65adf2f39a5a98507f1fe0ce10a2b8dbc/library/core/src/ops/function.rs:250:5
  24: std::sys_common::backtrace::__rust_begin_short_backtrace
             at /rustc/84c898d65adf2f39a5a98507f1fe0ce10a2b8dbc/library/std/src/sys_common/backtrace.rs:134:18
  25: std::rt::lang_start::{{closure}}
             at /rustc/84c898d65adf2f39a5a98507f1fe0ce10a2b8dbc/library/std/src/rt.rs:166:18
  26: core::ops::function::impls::<impl core::ops::function::FnOnce<A> for &F>::call_once
             at /rustc/84c898d65adf2f39a5a98507f1fe0ce10a2b8dbc/library/core/src/ops/function.rs:287:13
  27: std::panicking::try::do_call
             at /rustc/84c898d65adf2f39a5a98507f1fe0ce10a2b8dbc/library/std/src/panicking.rs:487:40
  28: std::panicking::try
             at /rustc/84c898d65adf2f39a5a98507f1fe0ce10a2b8dbc/library/std/src/panicking.rs:451:19
  29: std::panic::catch_unwind
             at /rustc/84c898d65adf2f39a5a98507f1fe0ce10a2b8dbc/library/std/src/panic.rs:140:14
  30: std::rt::lang_start_internal::{{closure}}
             at /rustc/84c898d65adf2f39a5a98507f1fe0ce10a2b8dbc/library/std/src/rt.rs:148:48
  31: std::panicking::try::do_call
             at /rustc/84c898d65adf2f39a5a98507f1fe0ce10a2b8dbc/library/std/src/panicking.rs:487:40
  32: std::panicking::try
             at /rustc/84c898d65adf2f39a5a98507f1fe0ce10a2b8dbc/library/std/src/panicking.rs:451:19
  33: std::panic::catch_unwind
             at /rustc/84c898d65adf2f39a5a98507f1fe0ce10a2b8dbc/library/std/src/panic.rs:140:14
  34: std::rt::lang_start_internal
             at /rustc/84c898d65adf2f39a5a98507f1fe0ce10a2b8dbc/library/std/src/rt.rs:148:20
  35: std::rt::lang_start
             at /rustc/84c898d65adf2f39a5a98507f1fe0ce10a2b8dbc/library/std/src/rt.rs:165:17
  36: _main
>', sink-parquet/src/lib.rs:94:72
stack backtrace:
   0: rust_begin_unwind
             at /rustc/84c898d65adf2f39a5a98507f1fe0ce10a2b8dbc/library/std/src/panicking.rs:579:5
   1: core::panicking::panic_fmt
             at /rustc/84c898d65adf2f39a5a98507f1fe0ce10a2b8dbc/library/core/src/panicking.rs:64:14
   2: core::result::unwrap_failed
             at /rustc/84c898d65adf2f39a5a98507f1fe0ce10a2b8dbc/library/core/src/result.rs:1750:5
   3: core::result::Result<T,E>::unwrap
             at /rustc/84c898d65adf2f39a5a98507f1fe0ce10a2b8dbc/library/core/src/result.rs:1090:23
   4: <apibara_sink_parquet::ParquetSink as apibara_sink_common::connector::Sink>::handle_data::{{closure}}
             at ./sink-parquet/src/lib.rs:94:22
   5: <core::pin::Pin<P> as core::future::future::Future>::poll
             at /rustc/84c898d65adf2f39a5a98507f1fe0ce10a2b8dbc/library/core/src/future/future.rs:125:9
   6: apibara_sink_common::connector::SinkConnector<F,B>::handle_message::{{closure}}
             at ./sink-common/src/connector.rs:250:25
   7: apibara_sink_common::connector::SinkConnector<F,B>::consume_stream::{{closure}}
             at ./sink-common/src/connector.rs:190:102
   8: apibara_sink_parquet::main::{{closure}}
             at ./sink-parquet/src/bin.rs:38:39
   9: tokio::runtime::park::CachedParkThread::block_on::{{closure}}
             at /Users/mac/.cargo/registry/src/github.com-1ecc6299db9ec823/tokio-1.28.2/src/runtime/park.rs:283:63
  10: tokio::runtime::coop::with_budget
             at /Users/mac/.cargo/registry/src/github.com-1ecc6299db9ec823/tokio-1.28.2/src/runtime/coop.rs:107:5
  11: tokio::runtime::coop::budget
             at /Users/mac/.cargo/registry/src/github.com-1ecc6299db9ec823/tokio-1.28.2/src/runtime/coop.rs:73:5
  12: tokio::runtime::park::CachedParkThread::block_on
             at /Users/mac/.cargo/registry/src/github.com-1ecc6299db9ec823/tokio-1.28.2/src/runtime/park.rs:283:31
  13: tokio::runtime::context::BlockingRegionGuard::block_on
             at /Users/mac/.cargo/registry/src/github.com-1ecc6299db9ec823/tokio-1.28.2/src/runtime/context.rs:315:13
  14: tokio::runtime::scheduler::multi_thread::MultiThread::block_on
             at /Users/mac/.cargo/registry/src/github.com-1ecc6299db9ec823/tokio-1.28.2/src/runtime/scheduler/multi_thread/mod.rs:66:9
  15: tokio::runtime::runtime::Runtime::block_on
             at /Users/mac/.cargo/registry/src/github.com-1ecc6299db9ec823/tokio-1.28.2/src/runtime/runtime.rs:304:45
  16: apibara_sink_parquet::main
             at ./sink-parquet/src/bin.rs:40:5
  17: core::ops::function::FnOnce::call_once
             at /rustc/84c898d65adf2f39a5a98507f1fe0ce10a2b8dbc/library/core/src/ops/function.rs:250:5
note: Some details are omitted, run with `RUST_BACKTRACE=full` for a verbose backtrace.
```
