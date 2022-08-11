//! # Sequencer
//!
//! The sequencer is used to track input and output sequence numbers.
//! All messages in the Apibara protocol contain a sequence number without
//! gaps. Chain reorganizations create an additional issue: the same sequence
//! number can repeat and all data following a reorged block must be
//! invalidated.
//!
//! The sequencer tracks input sequence numbers from multiple sources and
//! associates them with the output sequence number.
//! The sequencer can invalidate its output sequence numbers in response to
//! an input sequence invalidation.
//!
//! ## Example
//!
//! Imagine a system with three inputs `A`, `B`, and `C`.
//! Each input message is handled by an application that produces zero or more
//! output messages.
//! Notice that in this example is not concerned how input messages are received
//! or how outputs are produced. The sequencer is only involved in tracking and
//! mapping sequence numbers.
//!
//! The first message comes from `A` and has sequence `0`, the application produces
//! two messages. The diagram also includes the state of the input and output sequences.
//!
//! ```txt
//!  IN |A 0|
//!
//! OUT |O 0|O 1|
//!
//! INPUT SEQUENCE
//!   A: 0
//!
//! OUTPUT SEQUENCE: 1
//! ```
//!
//! Then it receives another message from `A`, this time producing a single output.
//!
//! ```txt
//!  IN |A 0|   |A 1|
//!
//! OUT |O 0|O 1|O 2|
//!
//! INPUT SEQUENCE
//!   A: 1
//!
//! OUTPUT SEQUENCE: 2
//! ```
//!
//! After several messages the state of the stream is the following:
//!
//! ```txt
//!  IN |A 0|   |A 1|B 0|B 1|A 2|       |C 0|B 2|
//!
//! OUT |O 0|O 1|O 2|   |O 3|O 4|O 5|O 6|O 7|O 8|O 9|
//!
//! INPUT SEQUENCE
//!   A: 2
//!   B: 2
//!   C: 0
//!
//! OUTPUT SEQUENCE: 9
//! ```
//!
//! Imagine that the stream receives a message invalidating all data produced by
//! `B` after (and including) sequence `1`. This is denoted as `Bx1` in the diagram.
//! The sequencer must rollback its state to just before receiving `B 1` for the first
//! time.
//!
//! ```txt
//!  IN |A 0|   |A 1|B 0|B 1|A 2|       |C 0|B 2|   |Bx1|
//!
//! OUT |O 0|O 1|O 2|   |O 3|O 4|O 5|O 6|O 7|O 8|O 9|
//!
//! INPUT SEQUENCE
//!   A: 1
//!   B: 0
//!
//! OUTPUT SEQUENCE: 2
//! ```
//!
//! Then the stream receives the new message `B'1` and operations resume.
//!
//! ```txt
//!  IN |A 0|   |A 1|B 0|B 1|A 2|       |C 0|B 2|   |Bx1|B'1|
//!
//! OUT |O 0|O 1|O 2|   |O 3|O 4|O 5|O 6|O 7|O 8|O 9|   |O 3|
//!
//! INPUT SEQUENCE
//!   A: 1
//!   B: 1
//!
//! OUTPUT SEQUENCE: 3
//! ```
