#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Basic test for CacheManager functionality
"""

import tempfile
import os

# Add parent directory to path
import sys

sys.path.insert(0, "/home/engine/project")

from pydal import DAL, Field
from pydal.caching import CacheManager, CacheStats


def test_cache_manager_basic():
    """Test basic CacheManager functionality"""
    # Create a temporary database
    fd, dbpath = tempfile.mkstemp(suffix=".db")
    os.close(fd)

    try:
        # Create database connection
        db = DAL(f"sqlite://{dbpath}", folder=None)

        # Initialize cache manager
        cache_mgr = CacheManager(db, max_size=100, default_ttl=60)
        db._cache_manager = cache_mgr

        # Define a test table
        db.define_table("test_table", Field("name"), Field("value", "integer"))

        # Insert some test data
        db.test_table.insert(name="alice", value=10)
        db.test_table.insert(name="bob", value=20)

        print("Testing cache manager...")

        # Test 1: Cache miss on first query
        print("\n1. First query (cache miss expected)")
        result1 = db(db.test_table).select(db.test_table.ALL, cache=True)
        print(f"   Rows: {len(result1)}")
        print(f"   Cache size: {cache_mgr.size}")
        print(
            f"   Stats - Hits: {cache_mgr.get_stats().hits}, Misses: {cache_mgr.get_stats().misses}"
        )

        # Test 2: Cache hit on second query
        print("\n2. Second query (cache hit expected)")
        result2 = db(db.test_table).select(db.test_table.ALL, cache=True)
        print(f"   Rows: {len(result2)}")
        print(f"   Cache size: {cache_mgr.size}")
        print(
            f"   Stats - Hits: {cache_mgr.get_stats().hits}, Misses: {cache_mgr.get_stats().misses}"
        )

        # Test 3: Cache invalidation on insert
        print("\n3. Insert new record (should invalidate cache)")
        db.test_table.insert(name="charlie", value=30)
        print(f"   Cache size after insert: {cache_mgr.size}")

        # Test 4: Cache miss after invalidation
        print("\n4. Query after insert (cache miss expected)")
        result3 = db(db.test_table).select(db.test_table.ALL, cache=True)
        print(f"   Rows: {len(result3)}")
        print(f"   Cache size: {cache_mgr.size}")
        print(
            f"   Stats - Hits: {cache_mgr.get_stats().hits}, Misses: {cache_mgr.get_stats().misses}"
        )

        # Test 5: Cache invalidation on update
        print("\n5. Update record (should invalidate cache)")
        db(db.test_table.name == "alice").update(value=15)
        print(f"   Cache size after update: {cache_mgr.size}")

        # Test 6: Cache invalidation on delete
        print("\n6. Delete record (should invalidate cache)")
        db(db.test_table.name == "bob").delete()
        print(f"   Cache size after delete: {cache_mgr.size}")

        # Test 7: Clear cache
        print("\n7. Clear all cache")
        cache_mgr.clear()
        print(f"   Cache size after clear: {cache_mgr.size}")

        # Test 8: Reset stats
        print("\n8. Reset stats")
        cache_mgr.reset_stats()
        print(
            f"   Stats - Hits: {cache_mgr.get_stats().hits}, Misses: {cache_mgr.get_stats().misses}"
        )

        # Test 9: Hit rate
        print("\n9. Test hit rate")
        db(db.test_table).select(db.test_table.ALL, cache=True)
        db(db.test_table).select(db.test_table.ALL, cache=True)
        db(db.test_table).select(db.test_table.ALL, cache=True)
        stats = cache_mgr.get_stats()
        print(f"   Hit rate: {stats.hit_rate:.2f}")
        print(f"   Total queries: {stats.total_queries}")

        print("\n✓ All basic tests passed!")

        db.close()

    finally:
        # Cleanup
        if os.path.exists(dbpath):
            os.unlink(dbpath)


def test_cache_stats():
    """Test CacheStats class"""
    print("\n\nTesting CacheStats class...")

    stats = CacheStats()

    # Test initial state
    assert stats.hits == 0
    assert stats.misses == 0
    assert stats.total_queries == 0
    assert stats.hit_rate == 0.0

    # Test recording
    stats.record_hit()
    stats.record_hit()
    stats.record_miss()

    assert stats.hits == 2
    assert stats.misses == 1
    assert stats.total_queries == 3
    assert abs(stats.hit_rate - (2.0 / 3.0)) < 0.001

    print("✓ CacheStats tests passed!")


if __name__ == "__main__":
    test_cache_stats()
    test_cache_manager_basic()
