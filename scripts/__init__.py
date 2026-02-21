import subprocess
import shutil
import os
import sys

def run_unit_tests():
    """Run unit tests in buildaquery/tests."""
    print("Running unit tests...")
    result = subprocess.run([sys.executable, "-m", "pytest", "buildaquery/tests"], check=False)
    sys.exit(result.returncode)

def run_integration_tests():
    """Run integration tests in tests/."""
    print("Running integration tests...")
    # Ensure Docker is running or handle failure gracefully if needed
    result = subprocess.run([sys.executable, "-m", "pytest", "tests"], check=False)
    sys.exit(result.returncode)

def run_all_tests():
    """Run all tests (unit + integration)."""
    print("Running all tests...")
    result = subprocess.run([sys.executable, "-m", "pytest"], check=False)
    sys.exit(result.returncode)

def clean_project():
    """Remove unnecessary folders like venv, __pycache__, and .pytest_cache."""
    folders_to_remove = [
        "venv",
        ".pytest_cache",
        "buildaquery/__pycache__",
        "buildaquery/abstract_syntax_tree/__pycache__",
        "buildaquery/compiler/__pycache__",
        "buildaquery/compiler/postgres/__pycache__",
        "buildaquery/execution/__pycache__",
        "buildaquery/traversal/__pycache__",
        "buildaquery/tests/__pycache__",
        "tests/__pycache__"
    ]
    
    # Also recursively find all __pycache__ directories
    for root, dirs, files in os.walk("."):
        if "__pycache__" in dirs:
            folders_to_remove.append(os.path.join(root, "__pycache__"))

    print("Cleaning up project...")
    for folder in set(folders_to_remove): # Use set to avoid duplicates
        if os.path.exists(folder):
            try:
                shutil.rmtree(folder)
                print(f"Removed: {folder}")
            except Exception as e:
                print(f"Failed to remove {folder}: {e}")
        else:
            # Check if it's a relative path that might exist
            pass
    
    print("Cleanup complete.")
