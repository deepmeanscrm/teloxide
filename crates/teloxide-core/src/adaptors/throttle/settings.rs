use std::{pin::Pin, sync::Arc};

use futures::{future::ready, Future};

// Required to not trigger `clippy::type-complexity` lint
type BoxedFnMut<I, O> = Box<dyn FnMut(I) -> O + Send>;
type BoxedFuture = Pin<Box<dyn Future<Output = ()> + Send>>;

/// Settings used by [`Throttle`] adaptor.
///
/// ## Examples
///
/// ```
/// use teloxide_core::adaptors::throttle;
///
/// let settings = throttle::Settings::default()
///     .on_queue_full(|pending| async move { /* do something when internal queue is full */ });
///
/// // use settings in `Throttle::with_settings` or other constructors
/// # let _ = settings;
/// ```
///
/// [`Throttle`]: crate::adaptors::throttle::Throttle
#[must_use]
#[non_exhaustive]
pub struct Settings {
    pub limits: Limits,
    pub on_queue_full: BoxedFnMut<usize, BoxedFuture>,
    pub retry: bool,
    pub check_slow_mode: bool,
    /// 中文注释：用于在 throttle 内部日志中标识“是哪个 bot 打出来的”。
    ///
    /// - 由于 teloxide-core 无法从 token 推导 bot 信息（也不应持有 token），
    ///   因此由上层在创建 Throttle 时注入一个上下文标签（例如 tg_bot_id /
    ///   bot_username）。
    pub context: Option<Arc<str>>,
}

/// Telegram request limits.
///
/// This struct is used in [`Throttle`].
///
/// Note that you may ask telegram [@BotSupport] to increase limits for your
/// particular bot if it has a lot of users (but they may or may not do that).
///
/// [@BotSupport]: https://t.me/botsupport
/// [`Throttle`]: crate::adaptors::throttle::Throttle
#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub struct Limits {
    /// Allowed messages in one chat per second.
    pub messages_per_sec_chat: u32,

    /// Allowed messages in one chat per minute.
    pub messages_per_min_chat: u32,

    /// Allowed messages in one channel or supergroup per minute.
    pub messages_per_min_channel_or_supergroup: u32,

    /// Allowed messages per second.
    pub messages_per_sec_overall: u32,
}

impl Settings {
    pub fn limits(mut self, val: Limits) -> Self {
        self.limits = val;
        self
    }

    pub fn on_queue_full<F, Fut>(mut self, mut val: F) -> Self
    where
        F: FnMut(usize) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.on_queue_full = Box::new(move |pending| Box::pin(val(pending)));
        self
    }

    pub fn no_retry(mut self) -> Self {
        self.retry = false;
        self
    }

    pub fn check_slow_mode(mut self) -> Self {
        self.check_slow_mode = true;
        self
    }

    /// 设置 throttle 日志上下文（例如：`tg_bot_id=123456 @mybot`）。
    pub fn context(mut self, val: impl Into<Arc<str>>) -> Self {
        self.context = Some(val.into());
        self
    }
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            limits: <_>::default(),
            on_queue_full: Box::new(|_pending| {
                // 中文注释：
                // - queue full 的日志由 worker 统一输出（带 context），这里保持默认 no-op；
                // - 若业务希望在 queue full 时做额外动作，可以显式设置 `on_queue_full(...)`。
                Box::pin(ready(()))
            }),
            retry: true,
            check_slow_mode: false,
            context: None,
        }
    }
}

/// Defaults are taken from [telegram documentation][tgdoc] (except for
/// `messages_per_min_channel`).
///
/// [tgdoc]: https://core.telegram.org/bots/faq#my-bot-is-hitting-limits-how-do-i-avoid-this
impl Default for Limits {
    fn default() -> Self {
        Self {
            messages_per_sec_chat: 1,
            messages_per_sec_overall: 30,
            messages_per_min_chat: 20,
            messages_per_min_channel_or_supergroup: 10,
        }
    }
}
