use crate::state::cell::AuthorityJournal;

/// Unified side-effect commit boundary for fs-meta runtime carriers.
///
/// Source/sink state carriers record authoritative mutations through this
/// boundary so business paths do not directly touch statecell journal details.
#[derive(Clone)]
pub(crate) struct CommitBoundary {
    authority: AuthorityJournal,
}

impl CommitBoundary {
    pub(crate) fn new(authority: AuthorityJournal) -> Self {
        Self { authority }
    }

    pub(crate) fn record(&self, op: &str, detail: impl Into<String>) {
        self.authority.append(op, detail.into());
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.authority.len()
    }
}
