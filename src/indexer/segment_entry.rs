use crate::common::BitSet;
use crate::core::SegmentDirectory;
use crate::core::SegmentId;
use crate::core::SegmentMeta;
use crate::directory::ManagedDirectory;
use crate::indexer::delete_queue::DeleteCursor;
use crate::Directory;
use std::fmt;

/// A segment entry describes the state of
/// a given segment, at a given instant.
///
/// In addition to segment `meta`,
/// it contains a few transient states
/// - `state` expresses whether the segment is already in the
/// middle of a merge
/// - `delete_bitset` is a bitset describing
/// documents that were deleted during the commit
/// itself.
/// - `delete_cursor` is the position in the delete queue.
/// Deletes happening before the cursor are reflected either
/// in the .del file or in the `delete_bitset`.
#[derive(Clone)]
pub struct SegmentEntry {
    meta: SegmentMeta,
    delete_bitset: Option<BitSet>,
    delete_cursor: DeleteCursor,
    directory: SegmentDirectory,
}

impl SegmentEntry {
    /// Create a new `SegmentEntry`
    pub(crate) fn new(
        segment_meta: SegmentMeta,
        delete_cursor: DeleteCursor,
        delete_bitset: Option<BitSet>,
        directory: SegmentDirectory,
    ) -> SegmentEntry {
        SegmentEntry {
            meta: segment_meta,
            delete_bitset,
            delete_cursor,
            directory,
        }
    }

    pub fn persist(&mut self, mut directory: ManagedDirectory) -> crate::Result<()> {
        //if let Some(volatile_directory) = self.volatile_directory.take() {}
        unimplemented!();
        self.directory = SegmentDirectory::Persisted(directory);
        Ok(())
    }

    /// Return a reference to the segment entry deleted bitset.
    ///
    /// `DocId` in this bitset are flagged as deleted.
    pub fn delete_bitset(&self) -> Option<&BitSet> {
        self.delete_bitset.as_ref()
    }

    /// Set the `SegmentMeta` for this segment.
    pub fn set_meta(&mut self, segment_meta: SegmentMeta) {
        self.meta = segment_meta;
    }

    /// Return a reference to the segment_entry's delete cursor
    pub fn delete_cursor(&mut self) -> &mut DeleteCursor {
        &mut self.delete_cursor
    }

    /// Returns the segment id.
    pub fn segment_id(&self) -> SegmentId {
        self.meta.id()
    }

    /// Accessor to the `SegmentMeta`
    pub fn meta(&self) -> &SegmentMeta {
        &self.meta
    }
}

impl fmt::Debug for SegmentEntry {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "SegmentEntry({:?})", self.meta)
    }
}
