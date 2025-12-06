#!/usr/bin/env python3
"""Helper script to set up .env file from .env.example."""
from pathlib import Path
import shutil

def setup_env():
    """Create .env file from .env.example if it doesn't exist."""
    env_file = Path(".env")
    env_example = Path(".env.example")
    
    if env_file.exists():
        print("✓ .env file already exists")
        return
    
    if not env_example.exists():
        print("✗ .env.example file not found")
        return
    
    # Copy example to .env
    shutil.copy(env_example, env_file)
    print("✓ Created .env file from .env.example")
    print("\n⚠️  IMPORTANT: Edit .env and add your OpenWeatherMap API key")
    print("   Get a free API key at: https://openweathermap.org/api")
    print(f"   File location: {env_file.absolute()}")

if __name__ == "__main__":
    setup_env()

