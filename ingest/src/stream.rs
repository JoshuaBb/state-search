use std::future::Future;

use futures::{Stream, StreamExt, TryStreamExt};
use tokio::sync::mpsc;

/// Drive a stream of futures through a bounded producer/consumer channel with output batching.
///
/// - **Producer**: polls up to `concurrency` futures at once (via `buffer_unordered`),
///   sending each resolved `Vec<Output>` into a bounded channel.
/// - **Consumer**: accumulates outputs from the channel and calls `flush` whenever
///   `batch_size` outputs accumulate, and once more at the end for any remainder.
///   After each `flush`, `on_flush` is called with the running total — use it for
///   progress logging or metrics (pass `|_| {}` to opt out).
///
/// The bounded channel (capacity `channel_cap`) provides backpressure so the producer
/// can only run `channel_cap` item-batches ahead of the consumer. Peak memory is
/// proportional to `(channel_cap + batch_size) × avg_output_size` rather than the
/// whole stream.
///
/// Returns the sum of counts returned by all `flush` calls.
pub async fn batched_channel_sink<Output, Fut, FlushFut>(
    stream: impl Stream<Item = Fut>,
    concurrency: usize,
    channel_cap: usize,
    batch_size: usize,
    flush: impl FnMut(Vec<Output>) -> FlushFut,
    on_flush: impl FnMut(u64),
) -> anyhow::Result<u64>
where
    Fut: Future<Output = anyhow::Result<Vec<Output>>>,
    FlushFut: Future<Output = anyhow::Result<u64>>,
{
    let (tx, mut rx) = mpsc::channel::<Vec<Output>>(channel_cap);
    let mut flush = flush;
    let mut on_flush = on_flush;

    // Moving `tx` into this block ensures it is dropped when the stream ends,
    // closing the channel and signalling the consumer to flush and finish.
    let producer = async move {
        stream
            .buffer_unordered(concurrency)
            .try_for_each(|outputs| {
                let tx = tx.clone();
                async move {
                    tx.send(outputs)
                        .await
                        .map_err(|_| anyhow::anyhow!("consumer dropped unexpectedly"))?;
                    Ok(())
                }
            })
            .await
    };

    let consumer = async move {
        let mut buf = Vec::with_capacity(batch_size);
        let mut total = 0u64;
        while let Some(outputs) = rx.recv().await {
            buf.extend(outputs);
            if buf.len() >= batch_size {
                total += flush(std::mem::take(&mut buf)).await?;
                on_flush(total);
            }
        }
        if !buf.is_empty() {
            total += flush(buf).await?;
            on_flush(total);
        }
        Ok::<u64, anyhow::Error>(total)
    };

    let (prod_result, cons_result) = tokio::join!(producer, consumer);
    // Prefer the consumer error (the root cause) over the producer's "channel closed" error.
    cons_result.and_then(|count| prod_result.map(|()| count))
}
