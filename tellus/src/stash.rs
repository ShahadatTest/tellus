use crate::{ActorId, Incoming, MailboxCapacity};
use std::{cmp, collections::VecDeque, mem, num::NonZeroUsize};

/// The items an actor has deferred, those it has handed back to its run loop, and the requests of
/// the item it is handling, which take effect once that item is settled.
pub(crate) struct Stash<M> {
    capacity: Option<NonZeroUsize>,

    // The unstashed items first, `unstashed` of them, then the stashed ones.
    items: VecDeque<Incoming<M>>,
    unstashed: usize,

    // The messages in `items` and `requested`: terminated signals skip the capacity.
    messages: usize,

    requested: Vec<Incoming<M>>,

    // How many of `requested` precede the last `unstash_all` request.
    release: Option<usize>,

    // How many leading stashed items an `unstash_all` made during the drain hands back.
    held: usize,

    // The length of `items` when the drain began.
    drain_len: usize,
}

impl<M> Stash<M> {
    pub(crate) fn new(capacity: MailboxCapacity) -> Self {
        let capacity = match capacity {
            MailboxCapacity::Unbounded => None,
            MailboxCapacity::Bounded(capacity) => Some(capacity),
        };

        Self {
            capacity,
            items: VecDeque::new(),
            unstashed: 0,
            messages: 0,
            requested: Vec::new(),
            release: None,
            held: 0,
            drain_len: 0,
        }
    }

    pub(crate) fn request_stash(&mut self, incoming: Incoming<M>) -> Result<(), Incoming<M>> {
        if matches!(incoming, Incoming::Message(_)) {
            if self
                .capacity
                .is_some_and(|capacity| self.messages >= capacity.get())
            {
                return Err(incoming);
            }
            self.messages += 1;
        }

        self.requested.push(incoming);
        Ok(())
    }

    pub(crate) fn request_unstash_all(&mut self) {
        self.release = Some(self.requested.len());
    }

    pub(crate) fn has_unstashed(&self) -> bool {
        self.unstashed > 0
    }

    pub(crate) fn pop_unstashed(&mut self) -> Option<Incoming<M>> {
        if self.unstashed == 0 {
            return None;
        }

        self.unstashed -= 1;
        let incoming = self
            .items
            .pop_front()
            .expect("every unstashed item is in the stash");
        if matches!(incoming, Incoming::Message(_)) {
            self.messages -= 1;
        }
        Some(incoming)
    }

    pub(crate) fn settle(&mut self, source: Source) {
        if self.requested.is_empty() && self.release.is_none() && self.held == 0 {
            return;
        }

        let release = self
            .release
            .take()
            .map(|requested| self.items.len() - self.unstashed + requested);
        self.items.extend(self.requested.drain(..));

        match source {
            Source::Stash => {
                if let Some(release) = release {
                    self.held = cmp::max(self.held, release);
                }

                if self.unstashed == 0 && self.held > 0 && self.items.len() < self.drain_len {
                    let held = mem::take(&mut self.held);
                    self.hand_back(held);
                }
            }

            Source::Mailbox => {
                let held = mem::take(&mut self.held);
                self.hand_back(cmp::max(held, release.unwrap_or_default()));
            }

            Source::Timer | Source::Start => {
                if let Some(release) = release {
                    self.hand_back(release);
                }
            }
        }
    }

    pub(crate) fn drop_signals_of(&mut self, actor_id: ActorId) {
        if self.items.len() + self.requested.len() == self.messages {
            return;
        }

        let is_signal = |incoming: &Incoming<M>| matches!(incoming, Incoming::Terminated(id) if *id == actor_id);

        let (unstashed, held) = (self.unstashed, self.held);
        let (mut dropped_unstashed, mut dropped_held) = (0, 0);
        let mut position = 0;
        self.items.retain(|incoming| {
            let index = position;
            position += 1;
            if !is_signal(incoming) {
                return true;
            }

            if index < unstashed {
                dropped_unstashed += 1;
            } else if index - unstashed < held {
                dropped_held += 1;
            }
            false
        });
        self.unstashed -= dropped_unstashed;
        self.held -= dropped_held;

        let release = self.release;
        let mut dropped_released = 0;
        let mut position = 0;
        self.requested.retain(|incoming| {
            let index = position;
            position += 1;
            if !is_signal(incoming) {
                return true;
            }

            if release.is_some_and(|release| index < release) {
                dropped_released += 1;
            }
            false
        });
        if let Some(release) = &mut self.release {
            *release -= dropped_released;
        }
    }

    pub(crate) fn hand_back_all(&mut self) -> Vec<Incoming<M>> {
        let discarded = mem::take(&mut self.requested);
        self.messages -= message_count(&discarded);
        self.release = None;
        self.held = 0;
        self.unstashed = self.items.len();
        self.drain_len = self.items.len();
        discarded
    }

    #[cfg(any(test, feature = "test-util"))]
    pub(crate) fn take_stashed(&mut self) -> Vec<Incoming<M>> {
        let stashed = self.items.drain(self.unstashed..).collect::<Vec<_>>();
        self.messages -= message_count(&stashed);
        self.held = 0;
        stashed
    }

    pub(crate) fn take_all(&mut self) -> Vec<Incoming<M>> {
        self.unstashed = 0;
        self.messages = 0;
        self.release = None;
        self.held = 0;
        self.items
            .drain(..)
            .chain(self.requested.drain(..))
            .collect()
    }

    fn hand_back(&mut self, stashed: usize) {
        if stashed == 0 {
            return;
        }

        self.unstashed += stashed;
        self.held = self.held.saturating_sub(stashed);
        self.drain_len = self.items.len();
    }
}

/// Where the item an actor has just handled came from, `Start` standing for `init` and
/// `recovered`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Source {
    Stash,
    Mailbox,
    Timer,
    Start,
}

fn message_count<M>(incomings: &[Incoming<M>]) -> usize {
    incomings
        .iter()
        .filter(|incoming| matches!(incoming, Incoming::Message(_)))
        .count()
}

#[cfg(test)]
mod tests {
    use crate::{
        ActorId, Incoming, MailboxCapacity,
        stash::{Source, Stash},
    };
    use std::num::NonZeroUsize;

    /// A full stash hands a message back, counting the unstashed items and the requests, while a
    /// terminated signal skips the capacity.
    #[test]
    fn a_full_stash_hands_a_message_back() {
        let mut stash = Stash::new(MailboxCapacity::Bounded(
            NonZeroUsize::new(2).expect("two is non-zero"),
        ));

        assert!(stash.request_stash(Incoming::Message(1)).is_ok());
        assert!(stash.request_stash(Incoming::Message(2)).is_ok());
        assert_eq!(
            stash.request_stash(Incoming::Message(3)),
            Err(Incoming::Message(3))
        );
        assert!(
            stash
                .request_stash(Incoming::Terminated(ActorId::new()))
                .is_ok()
        );

        stash.request_unstash_all();
        stash.settle(Source::Mailbox);
        assert_eq!(
            stash.request_stash(Incoming::Message(3)),
            Err(Incoming::Message(3))
        );
        assert_eq!(stash.pop_unstashed(), Some(Incoming::Message(1)));
        assert!(stash.request_stash(Incoming::Message(3)).is_ok());
    }

    /// The requests of one item take effect in the order they were made: an `unstash_all` hands
    /// back what was stashed before it, and an item stashed after it stays stashed.
    #[test]
    fn requests_take_effect_in_call_order() {
        let mut stash = Stash::new(MailboxCapacity::Unbounded);
        assert!(stash.request_stash(Incoming::Message(1)).is_ok());
        stash.settle(Source::Mailbox);
        assert_eq!(stash.pop_unstashed(), None);

        assert!(stash.request_stash(Incoming::Message(2)).is_ok());
        stash.request_unstash_all();
        assert!(stash.request_stash(Incoming::Message(3)).is_ok());
        assert_eq!(stash.pop_unstashed(), None);

        stash.settle(Source::Mailbox);
        assert_eq!(stash.pop_unstashed(), Some(Incoming::Message(1)));
        assert_eq!(stash.pop_unstashed(), Some(Incoming::Message(2)));
        assert_eq!(stash.pop_unstashed(), None);
        assert_eq!(stash.take_stashed(), [Incoming::Message(3)]);
    }

    /// A release made during a drain which stashes every item again waits for an item from the
    /// mailbox; a due timer does not count.
    #[test]
    fn a_release_during_a_drain_waits_for_the_mailbox() {
        let mut stash = Stash::new(MailboxCapacity::Unbounded);
        assert!(stash.request_stash(Incoming::Message(1)).is_ok());
        assert!(stash.request_stash(Incoming::Message(2)).is_ok());
        stash.request_unstash_all();
        stash.settle(Source::Mailbox);

        for n in [1, 2] {
            assert_eq!(stash.pop_unstashed(), Some(Incoming::Message(n)));
            assert!(stash.request_stash(Incoming::Message(n)).is_ok());
            stash.request_unstash_all();
            stash.settle(Source::Stash);
        }
        assert_eq!(stash.pop_unstashed(), None);

        stash.settle(Source::Timer);
        assert_eq!(stash.pop_unstashed(), None);

        stash.settle(Source::Mailbox);
        assert_eq!(stash.pop_unstashed(), Some(Incoming::Message(1)));
        assert_eq!(stash.pop_unstashed(), Some(Incoming::Message(2)));
    }

    /// A release made while receiving a due timer takes effect at once, a held one included, since
    /// a timer is no part of a drain.
    #[test]
    fn a_release_during_a_timer_takes_effect_at_once() {
        let mut stash = Stash::new(MailboxCapacity::Unbounded);
        assert!(stash.request_stash(Incoming::Message(1)).is_ok());
        stash.request_unstash_all();
        stash.settle(Source::Mailbox);

        assert_eq!(stash.pop_unstashed(), Some(Incoming::Message(1)));
        assert!(stash.request_stash(Incoming::Message(1)).is_ok());
        stash.request_unstash_all();
        stash.settle(Source::Stash);
        assert_eq!(stash.pop_unstashed(), None);

        assert!(stash.request_stash(Incoming::Message(2)).is_ok());
        stash.request_unstash_all();
        stash.settle(Source::Timer);
        assert_eq!(stash.pop_unstashed(), Some(Incoming::Message(1)));
        assert_eq!(stash.pop_unstashed(), Some(Incoming::Message(2)));
    }

    /// A release made during a drain which has shrunk the stash takes effect once the drain is
    /// over: of 3 waiting for 2 and 2 waiting for 1, 3 is handed back once 2 is handled.
    #[test]
    fn a_release_during_a_shrinking_drain_takes_effect_at_its_end() {
        let mut stash = Stash::new(MailboxCapacity::Unbounded);
        assert!(stash.request_stash(Incoming::Message(3)).is_ok());
        assert!(stash.request_stash(Incoming::Message(2)).is_ok());
        stash.settle(Source::Mailbox);
        stash.request_unstash_all();
        stash.settle(Source::Mailbox);

        assert_eq!(stash.pop_unstashed(), Some(Incoming::Message(3)));
        assert!(stash.request_stash(Incoming::Message(3)).is_ok());
        stash.request_unstash_all();
        stash.settle(Source::Stash);

        assert_eq!(stash.pop_unstashed(), Some(Incoming::Message(2)));
        stash.request_unstash_all();
        stash.settle(Source::Stash);

        assert_eq!(stash.pop_unstashed(), Some(Incoming::Message(3)));
        assert_eq!(stash.pop_unstashed(), None);
    }

    /// A restart hands every item back and discards the requests of the failed item.
    #[test]
    fn a_restart_hands_everything_back() {
        let mut stash = Stash::new(MailboxCapacity::Bounded(NonZeroUsize::MIN));
        assert!(stash.request_stash(Incoming::Message(1)).is_ok());
        stash.settle(Source::Mailbox);
        assert!(stash.request_stash(Incoming::Message(2)).is_err());

        stash.request_unstash_all();
        assert!(stash.hand_back_all().is_empty());
        assert_eq!(stash.pop_unstashed(), Some(Incoming::Message(1)));

        assert!(stash.request_stash(Incoming::Message(2)).is_ok());
        assert_eq!(stash.hand_back_all(), [Incoming::Message(2)]);
        assert_eq!(stash.pop_unstashed(), None);
        assert!(stash.request_stash(Incoming::Message(3)).is_ok());
    }

    /// Dropping the signals of an actor removes them from the unstashed and stashed items and from
    /// the requests, and a pending release still hands back the rest.
    #[test]
    fn dropping_the_signals_of_an_actor_keeps_everything_else() {
        let (dropped, kept) = (ActorId::new(), ActorId::new());
        let mut stash = Stash::new(MailboxCapacity::Unbounded);
        for incoming in [
            Incoming::Terminated(dropped),
            Incoming::Message(1),
            Incoming::Terminated(kept),
        ] {
            assert!(stash.request_stash(incoming).is_ok());
        }
        stash.request_unstash_all();
        stash.settle(Source::Mailbox);
        for incoming in [Incoming::Terminated(dropped), Incoming::Message(2)] {
            assert!(stash.request_stash(incoming).is_ok());
        }
        stash.settle(Source::Mailbox);
        for incoming in [Incoming::Message(3), Incoming::Terminated(dropped)] {
            assert!(stash.request_stash(incoming).is_ok());
        }
        stash.request_unstash_all();

        stash.drop_signals_of(dropped);
        stash.settle(Source::Mailbox);

        assert_eq!(stash.pop_unstashed(), Some(Incoming::Message(1)));
        assert_eq!(stash.pop_unstashed(), Some(Incoming::Terminated(kept)));
        assert_eq!(stash.pop_unstashed(), Some(Incoming::Message(2)));
        assert_eq!(stash.pop_unstashed(), Some(Incoming::Message(3)));
        assert_eq!(stash.pop_unstashed(), None);
    }

    /// Taking everything yields the unstashed items, then the stashed ones, then the requests.
    #[test]
    fn take_all_yields_unstashed_then_stashed_then_requested() {
        let mut stash = Stash::new(MailboxCapacity::Unbounded);
        assert!(stash.request_stash(Incoming::Message(1)).is_ok());
        stash.request_unstash_all();
        stash.settle(Source::Mailbox);
        assert!(stash.request_stash(Incoming::Message(2)).is_ok());
        stash.settle(Source::Mailbox);
        assert!(stash.request_stash(Incoming::Message(3)).is_ok());

        assert_eq!(
            stash.take_all(),
            [
                Incoming::Message(1),
                Incoming::Message(2),
                Incoming::Message(3)
            ]
        );
        assert_eq!(stash.pop_unstashed(), None);
    }
}
