//! Steering queue: user/bus input injected at turn boundaries without
//! interrupting in-flight tool execution (pi agent-loop.ts:257 semantics).

use std::collections::VecDeque;
use std::sync::Mutex;

use super::events::MessageItem;

#[derive(Default)]
pub struct Steering {
    queue: Mutex<VecDeque<MessageItem>>,
}

impl Steering {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push(&self, item: MessageItem) {
        self.queue.lock().unwrap().push_back(item);
    }

    pub fn drain(&self) -> Vec<MessageItem> {
        self.queue.lock().unwrap().drain(..).collect()
    }

    pub fn is_empty(&self) -> bool {
        self.queue.lock().unwrap().is_empty()
    }
}
