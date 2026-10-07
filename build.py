#!/usr/bin/env python3
"""
Script de compilation PyInstaller pour Melodi WebSocket.
"""
import sys
import shutil
import subprocess
from pathlib import Path

def build():
    root_dir = Path(__file__).resolve().parent
    print(f"Building melodi_websocket from {root_dir}...")

    # Nettoyage
    for folder in ("build", "dist"):
        p = root_dir / folder
        if p.exists():
            shutil.rmtree(p)

    cmd = [
        sys.executable,
        "-m",
        "PyInstaller",
        "--clean",
        str(root_dir / "melodi_websocket.spec")
    ]
    print(f"Executing: {' '.join(cmd)}")
    subprocess.check_call(cmd, cwd=root_dir)
    print("Build successful! Executable is in dist/melodi_websocket")

if __name__ == "__main__":
    build()
