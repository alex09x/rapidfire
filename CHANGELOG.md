# Changelog

## Unreleased

- Replace the waiter-list and block-pool mutexes with reusable atomic ownership
  slots. Retain overflow pages until channel destruction; cancellation forwards
  notifications without waiting for a notifier to resume.
- Rotate waiter selection and registration scans. Notification order between
  parked tasks is not guaranteed to be FIFO; message ordering is unchanged.
- Check slot reuse, cancellation, concurrent pool access and page growth with
  focused native, Loom and Miri tests. Document remaining progress limitations.

## 0.2.0

- Add `mpsc::bounded` and `mpsc::unbounded` for many producers and exactly one
  receiver. The sender remains cloneable; the receiver cannot be cloned and
  receives require an exclusive mutable borrow.
- Add `mpsc::Receiver::recv_many` to append ready messages to a reusable buffer,
  waiting only for the first message. A pending receive can be cancelled without
  consuming a message.
- Add a receive path that omits coordination between consumers, with batched
  capacity publication and sender notification. The general MPMC API is unchanged.
- Fix a shared send/receive lost-wakeup race: a completing operation could absorb
  a later notification and strand another waiter. Both MPMC and MPSC use the fix.
- Keep a bounded channel's approximate `len()` within its capacity when concurrent
  receives and refills occur between its two index observations.
- Extend correctness coverage for ownership, cancellation, block recycling,
  per-producer FIFO and wakeup races, including compile-fail, Loom and Miri checks.
- Add MPSC throughput and paced latency comparisons, and include the exclusive
  receiver in the common competitor benchmarks. Record individual samples and
  reject failed or incomplete requested CPU affinity.

The new receiver is opt-in. Its performance depends on capacity, payload size,
topology and batching; it is not uniformly faster than the general receiver.

## 0.1.0

- Initial release of bounded and unbounded asynchronous MPMC channels.
