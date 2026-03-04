use std::{
    collections::{HashMap, VecDeque},
    time::{Duration, Instant},
};

use tokio::sync::{mpsc, mpsc::error::TryRecvError, oneshot::Sender};
use vecrem::VecExt;

use crate::{
    adaptors::throttle::{request_lock::RequestLock, ChatIdHash, Limits, Settings},
    errors::AsResponseParameters,
    requests::Requester,
};

const MINUTE: Duration = Duration::from_secs(60);
const SECOND: Duration = Duration::from_secs(1);

/// Minimal time between calls to queue_full function
const QUEUE_FULL_DELAY: Duration = Duration::from_secs(4);

#[derive(Debug)]
pub(super) enum InfoMessage {
    GetLimits { response: Sender<Limits> },
    SetLimits { new: Limits, response: Sender<()> },
}

type RequestsSent = u32;

// I wish there was special data structure for history which removed the
// need in 2 hashmaps
// (waffle)
#[derive(Default)]
struct RequestsSentToChats {
    per_min: HashMap<ChatIdHash, RequestsSent>,
    per_sec: HashMap<ChatIdHash, RequestsSent>,
}

pub(super) struct FreezeUntil {
    pub(super) until: Instant,
    pub(super) after: Duration,
    pub(super) chat: ChatIdHash,
}

fn trim_deque(deque: &mut VecDeque<Instant>, cutoff: Instant) {
    while let Some(t) = deque.front().copied() {
        if t >= cutoff {
            break;
        }
        deque.pop_front();
    }
}

fn min_instant_opt(cur: Option<Instant>, next: Instant) -> Option<Instant> {
    match cur {
        Some(c) => Some(std::cmp::min(c, next)),
        None => Some(next),
    }
}

fn handle_info_message(req: InfoMessage, limits: &mut Limits) {
    // Errors are ignored with .ok(). Error means that the response channel
    // is closed and the response isn't needed.
    match req {
        InfoMessage::GetLimits { response } => response.send(*limits).ok(),
        InfoMessage::SetLimits { new, response } => {
            *limits = new;
            response.send(()).ok()
        }
    };
}

// Throttling is quite complicated. This comment describes the algorithm of the
// current implementation.
//
// ### Request
//
// When a throttling request is sent, it sends a tuple of `ChatId` and
// `Sender<()>` to the worker. Then the request waits for a notification from
// the worker. When notification is received, it sends the underlying request.
//
// ### Worker
//
// The worker does the most important job -- it ensures that the limits are
// never exceeded.
//
// The worker stores a history of requests sent in the last minute (and to which
// chats they were sent) and a queue of pending updates.
//
// The worker does the following algorithm loop:
//
// 1. If the queue is empty, wait for the first message in incoming channel (and
// add it to the queue).
//
// 2. Read all present messages from an incoming channel and transfer them to
// the queue.
//
// 3. Record the current time.
//
// 4. Clear the history from records whose time < (current time - minute).
//
// 5. Count all requests which were sent last second, `allowed =
// limit.messages_per_sec_overall - count`.
//
// 6. If `allowed == 0` wait a bit and `continue` to the next iteration.
//
// 7. Count how many requests were sent to which chats (i.e.: create
// `Map<ChatId, Count>`). (Note: the same map, but for last minute also exists,
// but it's updated, instead of recreation.)
//
// 8. While `allowed >= 0` search for requests which chat haven't exceed the
// limits (i.e.: map[chat] < limit), if one is found, decrease `allowed`, notify
// the request that it can be now executed, increase counts, add record to the
// history.
pub(super) async fn worker<B>(
    Settings { mut limits, mut on_queue_full, retry, check_slow_mode }: Settings,
    mut rx: mpsc::Receiver<(ChatIdHash, RequestLock)>,
    mut info_rx: mpsc::Receiver<InfoMessage>,
    bot: B,
) where
    B: Requester,
    B::Err: AsResponseParameters,
{
    // FIXME(waffle): Make an research about data structures for this queue.
    //                Currently this is O(n) removing (n = number of elements
    //                stayed), amortized O(1) push (vec+vecrem).
    let mut queue: Vec<(ChatIdHash, RequestLock)> =
        Vec::with_capacity(limits.messages_per_sec_overall as usize);

    // 中文注释：
    // - 严格滑动窗口（方案 A）：用 deque 记录“最近窗口内”的发送时间；
    // - 事件驱动：不再固定 tick（例如 250ms）轮询，而是“能发就立刻放行；不能发就 sleep_until(next_allowed_at)”；
    // - 这样在额度充足时可以瞬发，减少固定轮询带来的额外延迟抖动。
    let mut global_sec: VecDeque<Instant> = VecDeque::new();
    let mut per_chat_sec: HashMap<ChatIdHash, VecDeque<Instant>> = HashMap::new();
    let mut per_chat_min: HashMap<ChatIdHash, VecDeque<Instant>> = HashMap::new();

    let mut slow_mode: Option<HashMap<ChatIdHash, (Duration, Instant)>> =
        check_slow_mode.then(HashMap::new);

    let mut rx_is_closed = false;

    let mut last_queue_full =
        Instant::now().checked_sub(QUEUE_FULL_DELAY).unwrap_or_else(Instant::now);

    let (freeze_tx, mut freeze_rx) = mpsc::channel::<FreezeUntil>(1);

    while !rx_is_closed || !queue.is_empty() {
        // 中文注释：尽量及时响应 GetLimits / SetLimits。
        while let Ok(req) = info_rx.try_recv() {
            handle_info_message(req, &mut limits);
        }

        // 中文注释：优先处理 freeze（RetryAfter/慢速模式更新）；freeze 可能会 sleep。
        if let Ok(freeze_until) = freeze_rx.try_recv() {
            freeze(&mut freeze_rx, slow_mode.as_mut(), &bot, Some(freeze_until)).await;
        }

        // 尝试从 rx 把队列填充到 capacity（防 DOS：不超过 capacity）。
        if queue.is_empty() && !rx_is_closed {
            tokio::select! {
                // 有新请求时立即入队。
                req = rx.recv() => {
                    match req {
                        Some(r) => queue.push(r),
                        None => rx_is_closed = true,
                    }
                }
                // 处理 freeze/info，避免“队列空时阻塞导致无法 set_limits”。
                Some(freeze_until) = freeze_rx.recv() => {
                    freeze(&mut freeze_rx, slow_mode.as_mut(), &bot, Some(freeze_until)).await;
                    continue;
                }
                Some(info) = info_rx.recv() => {
                    handle_info_message(info, &mut limits);
                    continue;
                }
            }
        }

        while queue.len() < queue.capacity() {
            match rx.try_recv() {
                Ok(req) => queue.push(req),
                Err(TryRecvError::Disconnected) => {
                    rx_is_closed = true;
                    break;
                }
                Err(TryRecvError::Empty) => break,
            }
        }

        if queue.len() == queue.capacity() && last_queue_full.elapsed() > QUEUE_FULL_DELAY {
            last_queue_full = Instant::now();
            tokio::spawn(on_queue_full(queue.len()));
        }

        // _Maybe_ we need to use `spawn_blocking` here, because there is
        // decent amount of blocking work. However _for now_ I've decided not
        // to use it here.
        //
        // Reasons (not to use `spawn_blocking`):
        //
        // 1. The work seems not very CPU-bound, it's not heavy computations, it's more
        //    like light computations.
        //
        // 2. `spawn_blocking` is not zero-cost — it spawns a new system thread
        //    + do so other work. This may actually be *worse* then current
        //    "just do everything in this async fn" approach.
        //
        // 3. With `rt-threaded` feature, tokio uses [`num_cpus()`] threads which should
        //    be enough to work fine with one a-bit-blocking task. Crucially current
        //    behaviour will be problem mostly with single-threaded runtimes (and in
        //    case you're using one, you probably don't want to spawn unnecessary
        //    threads anyway).
        //
        // I think if we'll ever change this behaviour, we need to make it
        // _configurable_.
        //
        // See also [discussion (ru)].
        //
        // NOTE: If you are reading this because you have any problems because
        // of this worker, open an [issue on github]
        //
        // [`num_cpus()`]: https://vee.gg/JGwq2
        // [discussion (ru)]: https://t.me/rust_async/27891
        // [issue on github]: https://github.com/teloxide/teloxide/issues/new
        //
        // (waffle)

        let now = Instant::now();
        let min_back = now.checked_sub(MINUTE).unwrap_or(now);
        let sec_back = now.checked_sub(SECOND).unwrap_or(now);

        // 更新全局每秒滑动窗口。
        trim_deque(&mut global_sec, sec_back);

        // 全局每秒触顶：事件驱动 sleep_until 最早可发送时刻。
        if global_sec.len() as u32 >= limits.messages_per_sec_overall {
            let next_at = global_sec
                .front()
                .copied()
                .and_then(|t| t.checked_add(SECOND))
                .unwrap_or(now);
            if next_at > now {
                let sleep = tokio::time::sleep_until(next_at.into());
                tokio::pin!(sleep);
                tokio::select! {
                    _ = &mut sleep => {}
                    Some(freeze_until) = freeze_rx.recv() => {
                        freeze(&mut freeze_rx, slow_mode.as_mut(), &bot, Some(freeze_until)).await;
                    }
                    Some(info) = info_rx.recv() => {
                        handle_info_message(info, &mut limits);
                    }
                    req = rx.recv(), if queue.len() < queue.capacity() && !rx_is_closed => {
                        match req {
                            Some(r) => queue.push(r),
                            None => rx_is_closed = true,
                        }
                    }
                }
            }
            continue;
        }

        let mut queue_removing = queue.removing();
        let mut next_wake_at: Option<Instant> = None;
        let mut allowed_global = limits
            .messages_per_sec_overall
            .saturating_sub(global_sec.len() as u32);

        while allowed_global > 0 {
            let Some(entry) = queue_removing.next() else {
                break;
            };
            let chat = &entry.value().0;

            // 计算该 chat 下一次可放行的时间点（严格滑动窗口 + 慢速模式）。
            let mut chat_allowed_at = now;

            // 慢速模式：last + delay 之前不能发。
            if let Some(sm) = slow_mode
                .as_ref()
                .and_then(|m| m.get(chat).map(|(delay, last)| (delay, last)))
            {
                let ready_at = sm.1.checked_add(*sm.0).unwrap_or(*sm.1);
                chat_allowed_at = std::cmp::max(chat_allowed_at, ready_at);
            }

            // 单 chat 每秒窗口。
            if limits.messages_per_sec_chat > 0 {
                let mut remove_sec = false;
                let mut sec_ready_at: Option<Instant> = None;
                if let Some(deq) = per_chat_sec.get_mut(chat) {
                    trim_deque(deq, sec_back);
                    if deq.len() as u32 >= limits.messages_per_sec_chat {
                        if let Some(t0) = deq.front().copied() {
                            sec_ready_at = Some(t0.checked_add(SECOND).unwrap_or(t0));
                        }
                    }
                    remove_sec = deq.is_empty();
                }
                if remove_sec {
                    per_chat_sec.remove(chat);
                }
                if let Some(t) = sec_ready_at {
                    chat_allowed_at = std::cmp::max(chat_allowed_at, t);
                }
            }

            // 单 chat 每分钟窗口（普通 chat / 超群/频道不同上限）。
            let per_min_limit = if chat.is_channel_or_supergroup() {
                limits.messages_per_min_channel_or_supergroup
            } else {
                limits.messages_per_min_chat
            };
            if per_min_limit > 0 {
                let mut remove_min = false;
                let mut min_ready_at: Option<Instant> = None;
                if let Some(deq) = per_chat_min.get_mut(chat) {
                    trim_deque(deq, min_back);
                    if deq.len() as u32 >= per_min_limit {
                        if let Some(t0) = deq.front().copied() {
                            min_ready_at = Some(t0.checked_add(MINUTE).unwrap_or(t0));
                        }
                    }
                    remove_min = deq.is_empty();
                }
                if remove_min {
                    per_chat_min.remove(chat);
                }
                if let Some(t) = min_ready_at {
                    chat_allowed_at = std::cmp::max(chat_allowed_at, t);
                }
            }

            if chat_allowed_at <= now {
                let chat = *chat;
                let (_, lock) = entry.remove();
                // Only count request as sent if the request wasn't dropped before unlocked
                if lock.unlock(retry, freeze_tx.clone()).is_ok() {
                    let sent_at = Instant::now();

                    // 更新滑动窗口记录。
                    global_sec.push_back(sent_at);
                    per_chat_sec.entry(chat).or_default().push_back(sent_at);
                    per_chat_min.entry(chat).or_default().push_back(sent_at);

                    if let Some(sm) = slow_mode.as_mut().and_then(|m| m.get_mut(&chat)) {
                        sm.1 = sent_at;
                    }

                    allowed_global -= 1;

                    // 保护：全局每秒窗口可能触顶，提前结束本轮扫描。
                    if global_sec.len() as u32 >= limits.messages_per_sec_overall {
                        break;
                    }
                }
            } else if chat_allowed_at > now {
                next_wake_at = min_instant_opt(next_wake_at, chat_allowed_at);
            }
        }

        // 队列还有积压：计算下一次唤醒时间（最早可放行的那个）。
        if !queue.is_empty() {
            // 全局每秒触顶时，优先等待全局窗口释放。
            if global_sec.len() as u32 >= limits.messages_per_sec_overall {
                let next_at = global_sec
                    .front()
                    .copied()
                    .and_then(|t| t.checked_add(SECOND))
                    .unwrap_or(now);
                next_wake_at = Some(next_at);
            }

            if let Some(next_at) = next_wake_at {
                if next_at > Instant::now() {
                    let sleep = tokio::time::sleep_until(next_at.into());
                    tokio::pin!(sleep);
                    tokio::select! {
                        _ = &mut sleep => {}
                        Some(freeze_until) = freeze_rx.recv() => {
                            freeze(&mut freeze_rx, slow_mode.as_mut(), &bot, Some(freeze_until)).await;
                        }
                        Some(info) = info_rx.recv() => {
                            handle_info_message(info, &mut limits);
                        }
                        req = rx.recv(), if queue.len() < queue.capacity() && !rx_is_closed => {
                            match req {
                                Some(r) => queue.push(r),
                                None => rx_is_closed = true,
                            }
                        }
                    }
                }
            }
        }
    }
}

fn answer_info(rx: &mut mpsc::Receiver<InfoMessage>, limits: &mut Limits) {
    while let Ok(req) = rx.try_recv() {
        handle_info_message(req, limits);
    }
}

// FIXME: https://github.com/rust-lang/rust-clippy/issues/11610
#[allow(clippy::needless_pass_by_ref_mut)]
async fn freeze(
    rx: &mut mpsc::Receiver<FreezeUntil>,
    mut slow_mode: Option<&mut HashMap<ChatIdHash, (Duration, Instant)>>,
    bot: &impl Requester,
    mut imm: Option<FreezeUntil>,
) {
    while let Some(freeze_until) = imm.take().or_else(|| rx.try_recv().ok()) {
        let FreezeUntil { until, after, chat } = freeze_until;

        // Clippy thinks that this `.as_deref_mut()` doesn't change the type (&mut
        // HashMap -> &mut HashMap), but it's actually a reborrow (the lifetimes
        // differ), since we are in a loop, simply using `slow_mode` would produce a
        // moved-out error.
        #[allow(clippy::needless_option_as_deref)]
        if let Some(slow_mode) = slow_mode.as_deref_mut() {
            // TODO: do something with channels?...
            if let hash @ ChatIdHash::Id(id) = chat {
                // TODO: maybe not call `get_chat` every time?

                // At this point there isn't much we can do with the error besides ignoring
                if let Ok(chat) = bot.get_chat(id).await {
                    match chat.slow_mode_delay() {
                        Some(delay) => {
                            let now = Instant::now();
                            let new_delay = delay.duration();
                            slow_mode.insert(hash, (new_delay, now));
                        }
                        None => {
                            slow_mode.remove(&hash);
                        }
                    };
                }
            }
        }

        // slow mode is enabled and it is <= to the delay asked by telegram
        let slow_mode_enabled_and_likely_the_cause = slow_mode
            .as_ref()
            .and_then(|m| m.get(&chat).map(|(delay, _)| delay <= &after))
            .unwrap_or(false);

        // Do not sleep if slow mode is enabled since the freeze is most likely caused
        // by the said slow mode and not by the global limits.
        if !slow_mode_enabled_and_likely_the_cause {
            log::warn!(
                "freezing the bot for approximately {after:?} due to `RetryAfter` error from \
                 telegram"
            );

            tokio::time::sleep_until(until.into()).await;

            log::warn!("unfreezing the bot");
        }
    }
}

async fn read_from_rx<T>(rx: &mut mpsc::Receiver<T>, queue: &mut Vec<T>, rx_is_closed: &mut bool) {
    if queue.is_empty() {
        log::debug!("blocking on queue");

        match rx.recv().await {
            Some(req) => queue.push(req),
            None => *rx_is_closed = true,
        }
    }

    // Don't grow queue bigger than the capacity to limit DOS possibility
    while queue.len() < queue.capacity() {
        match rx.try_recv() {
            Ok(req) => queue.push(req),
            Err(TryRecvError::Disconnected) => {
                *rx_is_closed = true;
                break;
            }
            // There are no items in queue.
            Err(TryRecvError::Empty) => break,
        }
    }
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn issue_535() {
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);

        // Close channel
        drop(tx);

        // Previously this caused an infinite loop
        super::read_from_rx::<()>(&mut rx, &mut Vec::new(), &mut false).await;
    }
}
