# Apibara Streaming Protocol (v1alpha2)

The streaming protocol is used by Apibara nodes and clients to exchange data.

The goals are:
 - linearize non-linear data into something easy to follow,
 - let clients customize the data they want to receive, reducing the bandwidth required,
 - provide support for data at all stages of finality (pending, accepted, finalized).


## The protocol

The client starts by calling the `StreamData` method in the Stream gRPC service. The client must send a `StreamDataRequest` message to the server to start the stream.

The request includes:
 - `stream_id`: unique id for the stream. All messages generated in response to this request will have the specified stream id, or 0 if not specified.
 - `starting_cursor`: specifies from where to start the stream. The cursor is stream-specific.
 - `finality`: specifies the finality required by the client. This parameter changes the behavior of the stream.
 - `filter`: specifies what type of data the client wants to receive. This is specific to each stream.

After the client requests data, the stream will start sending `StreamDataResponse` messages to the client.
The messages can have the following content:

 - `invalidate`: invalidates data previously sent, for example in response to chain reorganizations.
 - `data`: sends a new batch of data.
 - `heartbeat`: periodically sent if no other types of messages were produced. Used to confirm that the client and server are still connected.

The client can reset the stream by sending a new `StreamDataRequest`. The server will stop sending data for the previous request and will start sending data for the new stream.
Notice that because the flow is async, the client may still receive messages from the old stream definition. Use the `stream_id` to uniquely identify streams.


### Data finality

Apibara supports streaming data with different finality. The stream behaves differently based on the finality mode specified in the request:

 - `finalized`: the stream sends `data` messages for finalized data only. The `data` messages contain the data requested in the stream filter. Notice that there cannot be `invalidate` messages in this type of stream.
 - `accepted`: the stream sends `data` messages for accepted data (including historical finalized data).
 - `pending`: the stream sends `data` messages for pending data and for accepted data. Notice that the client may receive the same pending data multiple times.


### Cursor

Cursors are used to identify a location in a stream. The `end_cursor` in `Data` messages refers to the location of the latest piece of data in the stream. When clients send this cursor as a `starting_cursor` in a request, the server will resume streaming by sending the first message that _follows_ the provided cursor. When no `starting_cursor` is specified, the stream will start from the genesis block.
If the data in or before `end_cursor` was invalidated (following a chain reorganization), the server will inform the client of it and will resume the stream from the new stream's head.

A `Cursor` is made of two components:

 - `order_key`: this is the sequence number for messages,
 - `unique_key`: this is used to discriminate between data generated from different blocks at the same height.
