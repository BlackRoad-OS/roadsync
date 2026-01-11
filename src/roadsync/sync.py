"""
RoadSync - Data Synchronization for BlackRoad
Real-time sync, conflict resolution, and offline-first patterns.
"""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set
import asyncio
import hashlib
import json
import logging
import threading
import time
import uuid

logger = logging.getLogger(__name__)


class SyncStatus(str, Enum):
    """Sync status."""
    PENDING = "pending"
    SYNCING = "syncing"
    SYNCED = "synced"
    CONFLICT = "conflict"
    ERROR = "error"


class ConflictStrategy(str, Enum):
    """Conflict resolution strategies."""
    LAST_WRITE_WINS = "last_write_wins"
    FIRST_WRITE_WINS = "first_write_wins"
    CLIENT_WINS = "client_wins"
    SERVER_WINS = "server_wins"
    MERGE = "merge"
    MANUAL = "manual"


@dataclass
class SyncRecord:
    """A syncable record."""
    id: str
    data: Dict[str, Any]
    version: int = 0
    checksum: str = ""
    created_at: datetime = field(default_factory=datetime.now)
    updated_at: datetime = field(default_factory=datetime.now)
    deleted: bool = False
    local_only: bool = False
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        if not self.checksum:
            self.checksum = self._compute_checksum()

    def _compute_checksum(self) -> str:
        data_str = json.dumps(self.data, sort_keys=True)
        return hashlib.md5(data_str.encode()).hexdigest()

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "data": self.data,
            "version": self.version,
            "checksum": self.checksum,
            "updated_at": self.updated_at.isoformat(),
            "deleted": self.deleted
        }


@dataclass
class SyncConflict:
    """A sync conflict."""
    id: str
    record_id: str
    local_record: SyncRecord
    remote_record: SyncRecord
    created_at: datetime = field(default_factory=datetime.now)
    resolved: bool = False
    resolution: Optional[str] = None


@dataclass
class SyncDelta:
    """Changes to sync."""
    created: List[SyncRecord] = field(default_factory=list)
    updated: List[SyncRecord] = field(default_factory=list)
    deleted: List[str] = field(default_factory=list)
    since_version: int = 0
    current_version: int = 0


class SyncStore:
    """Local sync store."""

    def __init__(self):
        self.records: Dict[str, SyncRecord] = {}
        self.pending_changes: List[SyncRecord] = []
        self.conflicts: Dict[str, SyncConflict] = {}
        self.version = 0
        self._lock = threading.Lock()

    def get(self, record_id: str) -> Optional[SyncRecord]:
        return self.records.get(record_id)

    def save(self, record: SyncRecord, track_change: bool = True) -> None:
        with self._lock:
            record.updated_at = datetime.now()
            record.version = self.version + 1
            record.checksum = record._compute_checksum()
            
            self.records[record.id] = record
            self.version = record.version
            
            if track_change:
                self.pending_changes.append(record)

    def delete(self, record_id: str, track_change: bool = True) -> bool:
        with self._lock:
            record = self.records.get(record_id)
            if record:
                record.deleted = True
                record.updated_at = datetime.now()
                record.version = self.version + 1
                self.version = record.version
                
                if track_change:
                    self.pending_changes.append(record)
                return True
            return False

    def get_pending_changes(self) -> List[SyncRecord]:
        return self.pending_changes.copy()

    def clear_pending_changes(self) -> None:
        with self._lock:
            self.pending_changes.clear()

    def get_delta(self, since_version: int) -> SyncDelta:
        """Get changes since a version."""
        delta = SyncDelta(since_version=since_version, current_version=self.version)
        
        for record in self.records.values():
            if record.version > since_version:
                if record.deleted:
                    delta.deleted.append(record.id)
                elif record.version == 1:
                    delta.created.append(record)
                else:
                    delta.updated.append(record)
        
        return delta

    def add_conflict(self, conflict: SyncConflict) -> None:
        with self._lock:
            self.conflicts[conflict.id] = conflict

    def resolve_conflict(self, conflict_id: str, resolution: SyncRecord) -> bool:
        with self._lock:
            conflict = self.conflicts.get(conflict_id)
            if conflict:
                conflict.resolved = True
                self.save(resolution, track_change=True)
                return True
            return False


class ConflictResolver:
    """Resolve sync conflicts."""

    def __init__(self, strategy: ConflictStrategy = ConflictStrategy.LAST_WRITE_WINS):
        self.strategy = strategy
        self.custom_resolvers: Dict[str, Callable] = {}

    def register_resolver(self, record_type: str, resolver: Callable) -> None:
        self.custom_resolvers[record_type] = resolver

    def resolve(self, local: SyncRecord, remote: SyncRecord) -> SyncRecord:
        """Resolve conflict between local and remote records."""
        record_type = local.metadata.get("type", "default")
        
        if record_type in self.custom_resolvers:
            return self.custom_resolvers[record_type](local, remote)

        if self.strategy == ConflictStrategy.LAST_WRITE_WINS:
            return remote if remote.updated_at > local.updated_at else local
        
        elif self.strategy == ConflictStrategy.FIRST_WRITE_WINS:
            return local if local.updated_at < remote.updated_at else remote
        
        elif self.strategy == ConflictStrategy.CLIENT_WINS:
            return local
        
        elif self.strategy == ConflictStrategy.SERVER_WINS:
            return remote
        
        elif self.strategy == ConflictStrategy.MERGE:
            return self._merge_records(local, remote)
        
        else:
            # Manual resolution required
            return None

    def _merge_records(self, local: SyncRecord, remote: SyncRecord) -> SyncRecord:
        """Merge two records."""
        merged_data = remote.data.copy()
        
        # Simple merge: local values override if updated more recently
        if local.updated_at > remote.updated_at:
            for key, value in local.data.items():
                merged_data[key] = value
        
        return SyncRecord(
            id=local.id,
            data=merged_data,
            version=max(local.version, remote.version) + 1,
            metadata=local.metadata
        )


class SyncEngine:
    """Core sync engine."""

    def __init__(
        self,
        store: SyncStore,
        resolver: ConflictResolver
    ):
        self.store = store
        self.resolver = resolver
        self._hooks: Dict[str, List[Callable]] = {
            "before_sync": [],
            "after_sync": [],
            "on_conflict": []
        }

    def add_hook(self, event: str, handler: Callable) -> None:
        if event in self._hooks:
            self._hooks[event].append(handler)

    def _trigger_hooks(self, event: str, *args) -> None:
        for handler in self._hooks.get(event, []):
            try:
                handler(*args)
            except Exception as e:
                logger.error(f"Sync hook error: {e}")

    async def push(self, remote_store: "SyncStore") -> SyncDelta:
        """Push local changes to remote."""
        self._trigger_hooks("before_sync", "push")
        
        pending = self.store.get_pending_changes()
        pushed = SyncDelta()
        conflicts = []

        for local_record in pending:
            remote_record = remote_store.get(local_record.id)
            
            if remote_record and remote_record.checksum != local_record.checksum:
                # Conflict detected
                if remote_record.version > local_record.version - 1:
                    resolved = self.resolver.resolve(local_record, remote_record)
                    
                    if resolved:
                        remote_store.save(resolved, track_change=False)
                        pushed.updated.append(resolved)
                    else:
                        conflict = SyncConflict(
                            id=str(uuid.uuid4()),
                            record_id=local_record.id,
                            local_record=local_record,
                            remote_record=remote_record
                        )
                        self.store.add_conflict(conflict)
                        self._trigger_hooks("on_conflict", conflict)
                        conflicts.append(conflict)
                else:
                    remote_store.save(local_record, track_change=False)
                    pushed.updated.append(local_record)
            else:
                remote_store.save(local_record, track_change=False)
                if remote_record:
                    pushed.updated.append(local_record)
                else:
                    pushed.created.append(local_record)

        self.store.clear_pending_changes()
        self._trigger_hooks("after_sync", "push", pushed)
        
        return pushed

    async def pull(self, remote_store: "SyncStore") -> SyncDelta:
        """Pull changes from remote."""
        self._trigger_hooks("before_sync", "pull")
        
        delta = remote_store.get_delta(self.store.version)
        conflicts = []

        for record in delta.created + delta.updated:
            local_record = self.store.get(record.id)
            
            if local_record and local_record.checksum != record.checksum:
                # Check for conflict
                if local_record in self.store.pending_changes:
                    resolved = self.resolver.resolve(local_record, record)
                    
                    if resolved:
                        self.store.save(resolved, track_change=False)
                    else:
                        conflict = SyncConflict(
                            id=str(uuid.uuid4()),
                            record_id=record.id,
                            local_record=local_record,
                            remote_record=record
                        )
                        self.store.add_conflict(conflict)
                        self._trigger_hooks("on_conflict", conflict)
                        conflicts.append(conflict)
                else:
                    self.store.save(record, track_change=False)
            else:
                self.store.save(record, track_change=False)

        for record_id in delta.deleted:
            self.store.delete(record_id, track_change=False)

        self._trigger_hooks("after_sync", "pull", delta)
        
        return delta


class SyncManager:
    """High-level sync management."""

    def __init__(
        self,
        conflict_strategy: ConflictStrategy = ConflictStrategy.LAST_WRITE_WINS
    ):
        self.local_store = SyncStore()
        self.resolver = ConflictResolver(conflict_strategy)
        self.engine = SyncEngine(self.local_store, self.resolver)
        self._auto_sync_task: Optional[asyncio.Task] = None
        self._running = False

    def create(self, record_id: str, data: Dict[str, Any], **metadata) -> SyncRecord:
        """Create a new record."""
        record = SyncRecord(
            id=record_id,
            data=data,
            metadata=metadata
        )
        self.local_store.save(record)
        return record

    def update(self, record_id: str, data: Dict[str, Any]) -> Optional[SyncRecord]:
        """Update a record."""
        record = self.local_store.get(record_id)
        if not record:
            return None
        
        record.data.update(data)
        self.local_store.save(record)
        return record

    def delete(self, record_id: str) -> bool:
        """Delete a record."""
        return self.local_store.delete(record_id)

    def get(self, record_id: str) -> Optional[SyncRecord]:
        """Get a record."""
        return self.local_store.get(record_id)

    def list(self, include_deleted: bool = False) -> List[SyncRecord]:
        """List all records."""
        records = list(self.local_store.records.values())
        if not include_deleted:
            records = [r for r in records if not r.deleted]
        return records

    async def sync(self, remote_store: SyncStore) -> Dict[str, Any]:
        """Perform full sync."""
        # Pull first
        pull_delta = await self.engine.pull(remote_store)
        
        # Then push
        push_delta = await self.engine.push(remote_store)
        
        return {
            "pulled": {
                "created": len(pull_delta.created),
                "updated": len(pull_delta.updated),
                "deleted": len(pull_delta.deleted)
            },
            "pushed": {
                "created": len(push_delta.created),
                "updated": len(push_delta.updated),
                "deleted": len(push_delta.deleted)
            },
            "conflicts": len(self.local_store.conflicts)
        }

    def get_conflicts(self) -> List[SyncConflict]:
        """Get unresolved conflicts."""
        return [c for c in self.local_store.conflicts.values() if not c.resolved]

    def resolve_conflict(self, conflict_id: str, choose: str = "local") -> bool:
        """Resolve a conflict."""
        conflict = self.local_store.conflicts.get(conflict_id)
        if not conflict:
            return False
        
        resolution = conflict.local_record if choose == "local" else conflict.remote_record
        return self.local_store.resolve_conflict(conflict_id, resolution)

    async def start_auto_sync(self, remote_store: SyncStore, interval: int = 30) -> None:
        """Start automatic sync."""
        self._running = True
        
        async def sync_loop():
            while self._running:
                try:
                    await self.sync(remote_store)
                except Exception as e:
                    logger.error(f"Auto sync error: {e}")
                await asyncio.sleep(interval)
        
        self._auto_sync_task = asyncio.create_task(sync_loop())

    async def stop_auto_sync(self) -> None:
        """Stop automatic sync."""
        self._running = False
        if self._auto_sync_task:
            self._auto_sync_task.cancel()


# Example usage
async def example_usage():
    """Example sync usage."""
    # Create managers
    client = SyncManager(conflict_strategy=ConflictStrategy.LAST_WRITE_WINS)
    server_store = SyncStore()

    # Create records locally
    client.create("user-1", {"name": "Alice", "email": "alice@example.com"})
    client.create("user-2", {"name": "Bob", "email": "bob@example.com"})

    print(f"Local records: {len(client.list())}")

    # Sync to server
    result = await client.sync(server_store)
    print(f"Sync result: {result}")

    # Update locally
    client.update("user-1", {"name": "Alice Smith"})

    # Sync again
    result = await client.sync(server_store)
    print(f"Sync after update: {result}")

    # Check server
    print(f"Server records: {len(server_store.records)}")
    print(f"Server user-1: {server_store.get('user-1').data}")
