//! 上游流空闲保护与绝对寿命保护（0026 废流治理三闸之闸 1 + 闸 2）。

use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::Bytes;
use futures::stream::BoxStream;
use futures::{Future, Stream};

/// 空闲超时错误文案的前缀
pub const IDLE_TIMEOUT_PREFIX: &str = "流式响应空闲超时";

/// 绝对寿命超时错误文案的前缀（0026 废流治理：不随分片重置，专治涓流型死挂长流）
pub const LIFETIME_TIMEOUT_PREFIX: &str = "流超过最大寿命";

/// 一条流错误文案是否为「挂死裁定」（空闲超时 / 绝对寿命）。
pub fn is_hung_verdict(text: &str) -> bool {
    text.starts_with(IDLE_TIMEOUT_PREFIX) || text.starts_with(LIFETIME_TIMEOUT_PREFIX)
}

/// 给一条上游字节流装上空闲守卫（`idle` 取设置页「流式响应空闲超时」）。
pub fn idle_guard(
    inner: BoxStream<'static, Result<Bytes, std::io::Error>>,
    idle: Duration,
) -> BoxStream<'static, Result<Bytes, std::io::Error>> {
    Box::pin(IdleGuard {
        inner,
        idle,
        sleep: Box::pin(tokio::time::sleep(idle)),
        done: false,
    })
}

struct IdleGuard {
    inner: BoxStream<'static, Result<Bytes, std::io::Error>>,
    idle: Duration,
    sleep: Pin<Box<tokio::time::Sleep>>,
    done: bool,
}

impl Stream for IdleGuard {
    type Item = Result<Bytes, std::io::Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.done {
            return Poll::Ready(None);
        }
        match self.inner.as_mut().poll_next(cx) {
            Poll::Ready(Some(item)) => {
                let deadline = tokio::time::Instant::now() + self.idle;
                self.sleep.as_mut().reset(deadline);
                Poll::Ready(Some(item))
            }
            Poll::Ready(None) => {
                self.done = true;
                Poll::Ready(None)
            }
            Poll::Pending => match self.sleep.as_mut().poll(cx) {
                Poll::Ready(()) => {
                    self.done = true;
                    Poll::Ready(Some(Err(std::io::Error::other(format!(
                        "{IDLE_TIMEOUT_PREFIX}({}秒)",
                        self.idle.as_secs()
                    )))))
                }
                Poll::Pending => Poll::Pending,
            },
        }
    }
}

/// 给一条上游字节流装上绝对寿命守卫（`lifetime` 取 `streamMaxLifetimeSeconds`）。
pub fn lifetime_guard(
    inner: BoxStream<'static, Result<Bytes, std::io::Error>>,
    lifetime: Duration,
) -> BoxStream<'static, Result<Bytes, std::io::Error>> {
    Box::pin(LifetimeGuard {
        inner,
        lifetime,
        sleep: Box::pin(tokio::time::sleep(lifetime)),
        done: false,
    })
}

struct LifetimeGuard {
    inner: BoxStream<'static, Result<Bytes, std::io::Error>>,
    lifetime: Duration,
    sleep: Pin<Box<tokio::time::Sleep>>,
    done: bool,
}

impl Stream for LifetimeGuard {
    type Item = Result<Bytes, std::io::Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.done {
            return Poll::Ready(None);
        }
        match self.inner.as_mut().poll_next(cx) {
            Poll::Ready(Some(item)) => Poll::Ready(item.into()),
            Poll::Ready(None) => {
                self.done = true;
                Poll::Ready(None)
            }
            Poll::Pending => match self.sleep.as_mut().poll(cx) {
                Poll::Ready(()) => {
                    self.done = true;
                    Poll::Ready(Some(Err(std::io::Error::other(format!(
                        "{LIFETIME_TIMEOUT_PREFIX}({}秒)",
                        self.lifetime.as_secs()
                    )))))
                }
                Poll::Pending => Poll::Pending,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{idle_guard, is_hung_verdict, lifetime_guard};
    use bytes::Bytes;
    use futures::stream::BoxStream;
    use futures::StreamExt;
    use std::time::Duration;

    fn boxed<S>(stream: S) -> BoxStream<'static, Result<Bytes, std::io::Error>>
    where
        S: futures::Stream<Item = Result<Bytes, std::io::Error>> + Send + 'static,
    {
        Box::pin(stream)
    }

    #[tokio::test]
    async fn lifetime_fires_despite_continuous_data() {
        let ticks = futures::stream::unfold(0u32, |n| async move {
            if n >= 100 {
                return None;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
            Some((Ok::<Bytes, std::io::Error>(Bytes::from_static(b"x")), n + 1))
        });
        let items: Vec<_> = lifetime_guard(boxed(ticks), Duration::from_millis(55))
            .collect()
            .await;
        let (oks, errs): (Vec<_>, Vec<_>) = items.into_iter().partition(|item| item.is_ok());
        assert!(!oks.is_empty());
        assert!(oks.len() < 100);
        assert_eq!(errs.len(), 1);
        let text = errs[0].as_ref().unwrap_err().to_string();
        assert!(is_hung_verdict(&text));
    }

    #[tokio::test]
    async fn idle_still_fires_on_silence() {
        let never = futures::stream::pending::<Result<Bytes, std::io::Error>>();
        let items: Vec<_> = idle_guard(boxed(never), Duration::from_millis(30)).collect().await;
        assert_eq!(items.len(), 1);
        assert!(is_hung_verdict(&items[0].as_ref().unwrap_err().to_string()));
    }
}
