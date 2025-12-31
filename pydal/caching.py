# -*- coding: utf-8 -*-
"""
Thread-safe LRU cache for pyDAL query results.

Provides CacheManager for caching query results with automatic
invalidation on table modifications.
"""

import hashlib
import pickle
import threading
import time
from collections import OrderedDict

from ._compat import hashlib_md5


class CacheStats:
    """
    Statistics for cache performance tracking.
    """

    def __init__(self):
        self._hits = 0
        self._misses = 0
        self._lock = threading.RLock()

    def record_hit(self):
        with self._lock:
            self._hits += 1

    def record_miss(self):
        with self._lock:
            self._misses += 1

    @property
    def hits(self):
        with self._lock:
            return self._hits

    @property
    def misses(self):
        with self._lock:
            return self._misses

    @property
    def total_queries(self):
        with self._lock:
            return self._hits + self._misses

    @property
    def hit_rate(self):
        with self._lock:
            total = self._hits + self._misses
            if total == 0:
                return 0.0
            return float(self._hits) / total


class _CacheEntry:
    """
    Internal cache entry wrapper.
    """

    __slots__ = ("value", "timestamp", "ttl", "tables", "key")

    def __init__(self, value, timestamp, ttl, tables, key):
        self.value = value
        self.timestamp = timestamp
        self.ttl = ttl
        self.tables = tables
        self.key = key

    def is_expired(self, now):
        if self.ttl is None:
            return False
        return (now - self.timestamp) > self.ttl


class CacheManager:
    """
    Thread-safe LRU cache manager for query results.

    Args:
        db: The DAL database instance
        max_size (int): Maximum number of cache entries. Default: 1000
        default_ttl (int): Default time-to-live in seconds. Default: 300 (5 minutes)
    """

    def __init__(self, db, max_size=1000, default_ttl=300):
        self._db = db
        self._max_size = max_size
        self._default_ttl = default_ttl
        self._cache = OrderedDict()
        self._stats = CacheStats()
        self._lock = threading.RLock()
        self._table_index = {}  # Maps table_name -> set of cache keys

    def _generate_key(self, query, fields, attrs):
        """
        Generate a cache key from query, fields, and attributes.
        """
        # Serialize the query components for hashing
        key_parts = []

        # Serialize query
        if query is not None:
            try:
                key_parts.append(str(query))
            except:
                key_parts.append(repr(query))
        else:
            key_parts.append("None")

        # Serialize fields
        fields_serialized = []
        for field in fields:
            try:
                fields_serialized.append(str(field))
            except:
                fields_serialized.append(repr(field))
        key_parts.append("|".join(sorted(fields_serialized)))

        # Serialize attributes (excluding cache-related ones)
        attrs_filtered = {}
        for k, v in attrs.items():
            if k not in ("cache", "cacheable", "cache_model", "cache_time"):
                try:
                    attrs_filtered[k] = str(v)
                except:
                    attrs_filtered[k] = repr(v)
        key_parts.append(str(sorted(attrs_filtered.items())))

        key_string = "|".join(key_parts)
        return hashlib_md5(key_string).hexdigest()

    def _extract_tables(self, query, fields, attrs):
        """
        Extract table names from query and fields.
        """
        tables = set()

        # Extract from query
        if query is not None:
            try:
                # Query object has tables accessible through adapter
                adapter = self._db._adapter
                tablemap = adapter.tables(
                    query,
                    attrs.get("join", None),
                    attrs.get("left", None),
                    attrs.get("orderby", None),
                    attrs.get("groupby", None),
                )
                tables.update(tablemap.keys())
            except:
                pass

        # Extract from fields
        for field in fields:
            try:
                if hasattr(field, "_tablename"):
                    tables.add(field._tablename)
                elif hasattr(field, "table"):
                    tables.add(field.table._tablename)
            except:
                pass

        return frozenset(tables)

    def _evict_if_needed(self):
        """
        Evict oldest entries if cache exceeds max_size.
        """
        while len(self._cache) >= self._max_size:
            oldest_key, oldest_entry = self._cache.popitem(last=False)
            # Remove from table index
            for table in oldest_entry.tables:
                if table in self._table_index:
                    self._table_index[table].discard(oldest_key)
                    if not self._table_index[table]:
                        del self._table_index[table]

    def _cleanup_expired(self):
        """
        Remove expired entries.
        """
        now = time.time()
        expired_keys = []

        for key, entry in self._cache.items():
            if entry.is_expired(now):
                expired_keys.append(key)

        for key in expired_keys:
            entry = self._cache.pop(key, None)
            if entry:
                for table in entry.tables:
                    if table in self._table_index:
                        self._table_index[table].discard(key)
                        if not self._table_index[table]:
                            del self._table_index[table]

    def cache_query(self, query, fields, attrs, ttl=None):
        """
        Cache and retrieve query results using LRU eviction.

        Args:
            query: The query object or None
            fields: List of fields to select
            attrs: Dictionary of query attributes
            ttl: Time-to-live in seconds. Uses default_ttl if None

        Returns:
            The cached query result or executes query if not cached
        """
        with self._lock:
            key = self._generate_key(query, fields, attrs)

            # Check cache
            if key in self._cache:
                entry = self._cache[key]
                now = time.time()

                if not entry.is_expired(now):
                    # Cache hit - move to end (most recently used)
                    self._cache.move_to_end(key)
                    self._stats.record_hit()
                    return entry.value
                else:
                    # Expired - remove it
                    self._remove_entry(key)

            # Cache miss
            self._stats.record_miss()

        # Execute query outside lock
        adapter = self._db._adapter
        colnames, sql = adapter._select_wcols(query, fields, **attrs)
        result = adapter._select_aux(sql, fields, attrs, colnames)

        # Store result
        with self._lock:
            # Check cleanup needed periodically
            if len(self._cache) > 0 and len(self._cache) % 100 == 0:
                self._cleanup_expired()

            tables = self._extract_tables(query, fields, attrs)
            effective_ttl = ttl if ttl is not None else self._default_ttl

            entry = _CacheEntry(
                value=result,
                timestamp=time.time(),
                ttl=effective_ttl,
                tables=tables,
                key=key,
            )

            # Ensure capacity
            self._evict_if_needed()

            # Store in cache
            self._cache[key] = entry

            # Update table index
            for table in tables:
                if table not in self._table_index:
                    self._table_index[table] = set()
                self._table_index[table].add(key)

        return result

    def invalidate_table(self, table_name):
        """
        Invalidate all cache entries that depend on a specific table.

        Args:
            table_name (str): Name of the table to invalidate
        """
        with self._lock:
            if table_name not in self._table_index:
                return

            keys_to_remove = list(self._table_index[table_name])

            for key in keys_to_remove:
                entry = self._cache.pop(key, None)
                if entry:
                    # Remove from all table indices
                    for table in entry.tables:
                        if table in self._table_index:
                            self._table_index[table].discard(key)
                            if not self._table_index[table]:
                                del self._table_index[table]

    def get_stats(self):
        """
        Get cache statistics.

        Returns:
            CacheStats: Object with hits, misses, total_queries, and hit_rate
        """
        return self._stats

    def clear(self):
        """
        Clear all cached entries.
        """
        with self._lock:
            self._cache.clear()
            self._table_index.clear()

    def reset_stats(self):
        """
        Reset cache statistics counters to zero.
        """
        with self._lock:
            with self._stats._lock:
                self._stats._hits = 0
                self._stats._misses = 0

    def _remove_entry(self, key):
        """
        Remove a cache entry by key.
        """
        entry = self._cache.pop(key, None)
        if entry:
            for table in entry.tables:
                if table in self._table_index:
                    self._table_index[table].discard(key)
                    if not self._table_index[table]:
                        del self._table_index[table]

    @property
    def size(self):
        """
        Current number of cached entries.
        """
        with self._lock:
            return len(self._cache)

    @property
    def max_size(self):
        """
        Maximum cache size.
        """
        return self._max_size

    @property
    def default_ttl(self):
        """
        Default time-to-live in seconds.
        """
        return self._default_ttl
