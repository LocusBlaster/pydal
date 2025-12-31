# -*- coding: utf-8 -*-
"""
Query result caching with LRU eviction and automatic invalidation
"""

import hashlib
import pickle
import threading
import time
from collections import OrderedDict

from ._compat import hashlib_md5, iteritems


class CacheStats:
    """Statistics for cache operations"""

    def __init__(self, hits=0, misses=0):
        self._hits = hits
        self._misses = misses

    @property
    def hits(self):
        return self._hits

    @property
    def misses(self):
        return self._misses

    @property
    def total_queries(self):
        return self._hits + self._misses

    @property
    def hit_rate(self):
        total = self.total_queries
        if total == 0:
            return 0.0
        return self._hits / float(total)

    def __repr__(self):
        return (
            f"CacheStats(hits={self.hits}, misses={self.misses}, "
            f"total_queries={self.total_queries}, hit_rate={self.hit_rate:.2f})"
        )


class CacheManager:
    """
    Thread-safe LRU cache manager for query results with automatic invalidation.

    Features:
    - LRU eviction when max_size is reached
    - TTL-based expiration
    - Table dependency tracking for automatic invalidation
    - Thread-safe operations
    - Query fingerprinting based on structure
    """

    def __init__(self, db, max_size=1000, default_ttl=300):
        """
        Initialize the cache manager.

        Args:
            db: Database connection (DAL instance)
            max_size: Maximum number of cache entries (default: 1000)
            default_ttl: Default time-to-live in seconds (default: 300)
        """
        self.db = db
        self.max_size = max_size
        self.default_ttl = default_ttl
        self._cache = OrderedDict()
        self._table_dependencies = {}
        self._lock = threading.RLock()
        self._hits = 0
        self._misses = 0

    def _generate_cache_key(self, query, fields, attributes):
        """
        Generate a unique cache key based on query structure.

        This is complex because it needs to:
        1. Parse the query expression tree
        2. Extract table dependencies
        3. Consider field ordering and attributes
        4. Handle joins, subqueries, and common filters
        """
        key_parts = []

        # Add query structure
        if query:
            query_str = self._serialize_query(query)
            key_parts.append(query_str)

        # Add fields (order matters for cache differentiation)
        field_names = []
        for field in fields:
            if hasattr(field, "_tablename") and hasattr(field, "name"):
                field_names.append(f"{field._tablename}.{field.name}")
            elif hasattr(field, "__str__"):
                field_names.append(str(field))
        key_parts.append(":".join(field_names))

        # Add critical attributes that affect results
        critical_attrs = [
            "orderby",
            "groupby",
            "limitby",
            "distinct",
            "having",
            "join",
            "left",
            "offset",
        ]
        for attr in critical_attrs:
            if attr in attributes and attributes[attr] is not None:
                val = attributes[attr]
                if attr in ("orderby", "groupby", "having"):
                    val_str = self._serialize_expression(val)
                elif attr == "join" or attr == "left":
                    val_str = self._serialize_join(val)
                else:
                    val_str = str(val)
                key_parts.append(f"{attr}={val_str}")

        # Generate hash of the key
        key_data = "|".join(key_parts)
        cache_key = hashlib_md5(key_data).hexdigest()

        return cache_key, key_data

    def _serialize_query(self, query):
        """
        Serialize a query expression into a canonical string representation.
        Handles nested expressions, operators, and table references.
        """
        if query is None:
            return "None"

        from .objects import Expression, Field, Query

        if isinstance(query, Query):
            return self._serialize_expression(query)
        elif isinstance(query, Expression):
            return self._serialize_expression(query)
        elif isinstance(query, Field):
            return f"Field:{query._tablename}.{query.name}"
        else:
            return str(query)

    def _serialize_expression(self, expr):
        """Serialize an expression recursively."""
        if expr is None:
            return "None"

        from .objects import Expression, Field

        if isinstance(expr, Field):
            return f"Field:{expr._tablename}.{expr.name}"
        elif isinstance(expr, Expression):
            parts = [str(expr.op)]
            if hasattr(expr, "first") and expr.first is not None:
                parts.append(self._serialize_expression(expr.first))
            if hasattr(expr, "second") and expr.second is not None:
                parts.append(self._serialize_expression(expr.second))
            return f"Expr({','.join(parts)})"
        elif isinstance(expr, (list, tuple)):
            return "[" + ",".join(self._serialize_expression(e) for e in expr) + "]"
        else:
            return str(expr)

    def _serialize_join(self, join):
        """Serialize join conditions."""
        if join is None:
            return "None"
        elif isinstance(join, (list, tuple)):
            return "[" + ",".join(self._serialize_expression(j) for j in join) + "]"
        else:
            return self._serialize_expression(join)

    def _extract_tables_from_query(self, query, fields, attributes):
        """
        Extract all table names that this query depends on.
        This is critical for invalidation.
        """
        tables = set()

        # Extract from query
        if query:
            self._extract_tables_from_expression(query, tables)

        # Extract from fields
        for field in fields:
            if hasattr(field, "_tablename"):
                tables.add(field._tablename)

        # Extract from joins
        for attr in ("join", "left"):
            if attr in attributes and attributes[attr]:
                join_list = attributes[attr]
                if not isinstance(join_list, (list, tuple)):
                    join_list = [join_list]
                for join in join_list:
                    self._extract_tables_from_expression(join, tables)

        # Extract from orderby/groupby
        for attr in ("orderby", "groupby", "having"):
            if attr in attributes and attributes[attr]:
                self._extract_tables_from_expression(attributes[attr], tables)

        return list(tables)

    def _extract_tables_from_expression(self, expr, tables):
        """Recursively extract table names from an expression."""
        if expr is None:
            return

        from .objects import Expression, Field, Table

        if isinstance(expr, Field):
            tables.add(expr._tablename)
        elif isinstance(expr, Table):
            tables.add(expr._tablename)
        elif isinstance(expr, Expression):
            if hasattr(expr, "first") and expr.first is not None:
                self._extract_tables_from_expression(expr.first, tables)
            if hasattr(expr, "second") and expr.second is not None:
                self._extract_tables_from_expression(expr.second, tables)
        elif isinstance(expr, (list, tuple)):
            for item in expr:
                self._extract_tables_from_expression(item, tables)

    def _evict_lru(self):
        """Evict the least recently used entry."""
        if self._cache:
            cache_key, (_, _, dep_tables, _) = self._cache.popitem(last=False)
            # Clean up table dependencies
            for table in dep_tables:
                if table in self._table_dependencies:
                    self._table_dependencies[table].discard(cache_key)
                    if not self._table_dependencies[table]:
                        del self._table_dependencies[table]

    def cache_query(self, query, fields, attributes):
        """
        Execute and cache a query, or return cached result.

        Args:
            query: The query object
            fields: List of fields to select
            attributes: Query attributes (orderby, groupby, etc.)

        Returns:
            Query results (Rows object)
        """
        # Generate cache key and extract dependencies
        cache_key, key_data = self._generate_cache_key(query, fields, attributes)
        dep_tables = self._extract_tables_from_query(query, fields, attributes)

        with self._lock:
            # Check if cached and not expired
            if cache_key in self._cache:
                timestamp, result, tables, ttl = self._cache[cache_key]
                current_time = time.time()

                if ttl is None or (current_time - timestamp) < ttl:
                    # Move to end (mark as recently used)
                    self._cache.move_to_end(cache_key)
                    self._hits += 1
                    # Return a copy to prevent external modifications
                    return self._copy_result(result)
                else:
                    # Expired, remove it
                    self._remove_cache_entry(cache_key)

            # Cache miss - execute query
            self._misses += 1

        # Execute query without holding lock (allow parallel queries)
        # Call _select_aux directly to avoid recursion through cache manager
        adapter = self.db._adapter
        # Remove cache attribute to prevent _select_aux from trying to use it
        exec_attributes = dict(attributes)
        exec_attributes.pop("cache", None)
        
        # Expand fields to handle SQLALL objects (like db.table.ALL)
        # This must be done before calling _select_wcols to avoid AttributeError
        tablenames = adapter.tables(
            query,
            exec_attributes.get("join", None),
            exec_attributes.get("left", None),
            exec_attributes.get("orderby", None),
            exec_attributes.get("groupby", None),
        )
        expanded_fields = adapter.expand_all(fields, tablenames)
        
        colnames, sql = adapter._select_wcols(query, expanded_fields, **exec_attributes)
        result = adapter._select_aux(sql, expanded_fields, exec_attributes, colnames)

        # Store result
        with self._lock:
            # Check size and evict if necessary
            while len(self._cache) >= self.max_size:
                self._evict_lru()

            # Get TTL from attributes or use default
            ttl = attributes.get("cache_ttl", self.default_ttl)

            # Store the result
            self._cache[cache_key] = (time.time(), result, dep_tables, ttl)

            # Update table dependencies
            for table in dep_tables:
                if table not in self._table_dependencies:
                    self._table_dependencies[table] = set()
                self._table_dependencies[table].add(cache_key)

        return self._copy_result(result)

    def _copy_result(self, result):
        """
        Create a copy of the result to prevent external modifications.
        Uses pickle for deep copy of Rows objects.
        """
        try:
            return pickle.loads(pickle.dumps(result, pickle.HIGHEST_PROTOCOL))
        except Exception:
            # If pickle fails, return original (best effort)
            return result

    def _remove_cache_entry(self, cache_key):
        """Remove a cache entry and clean up its dependencies."""
        if cache_key in self._cache:
            _, _, dep_tables, _ = self._cache[cache_key]
            del self._cache[cache_key]

            # Clean up table dependencies
            for table in dep_tables:
                if table in self._table_dependencies:
                    self._table_dependencies[table].discard(cache_key)
                    if not self._table_dependencies[table]:
                        del self._table_dependencies[table]

    def invalidate_table(self, table_name):
        """
        Invalidate all cache entries that depend on the given table.

        Args:
            table_name: Name of the table to invalidate
        """
        with self._lock:
            if table_name in self._table_dependencies:
                # Get all cache keys that depend on this table
                cache_keys = list(self._table_dependencies[table_name])

                # Remove each cache entry
                for cache_key in cache_keys:
                    self._remove_cache_entry(cache_key)

    def clear(self):
        """Clear all cache entries."""
        with self._lock:
            self._cache.clear()
            self._table_dependencies.clear()

    def reset_stats(self):
        """Reset hit/miss counters to zero."""
        with self._lock:
            self._hits = 0
            self._misses = 0

    def get_stats(self):
        """
        Get cache statistics.

        Returns:
            CacheStats object with hits, misses, total_queries, hit_rate
        """
        with self._lock:
            return CacheStats(hits=self._hits, misses=self._misses)

    def __repr__(self):
        with self._lock:
            return (
                f"CacheManager(max_size={self.max_size}, "
                f"cached_entries={len(self._cache)}, "
                f"stats={self.get_stats()})"
            )
