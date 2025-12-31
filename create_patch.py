#!/usr/bin/env python
"""
Create solution.patch combining all changes
"""

import subprocess
import os

# Create the patch
print("Creating solution.patch...")

# Stage all relevant files
subprocess.run(
    ["git", "add", "pydal/caching.py", "pydal/base.py", "pydal/objects.py"],
    cwd="/home/engine/project",
    check=True,
)

# Create the diff
result = subprocess.run(
    ["git", "diff", "--cached", "--no-color", "HEAD"],
    cwd="/home/engine/project",
    capture_output=True,
    text=True,
    check=True,
)

with open("/home/engine/project/solution.patch", "w") as f:
    f.write(result.stdout)

print(f"Patch created successfully!")
print(f"Lines: {len(result.stdout.splitlines())}")

# Verify
print("\nVerifying patch...")
subprocess.run(["git", "reset", "HEAD"], cwd="/home/engine/project", check=True)

print("\nPatch file: /home/engine/project/solution.patch")
print("First 50 lines:")
with open("/home/engine/project/solution.patch") as f:
    lines = f.readlines()[:50]
    for i, line in enumerate(lines, 1):
        print(f"{i:3d}: {line}", end="")
