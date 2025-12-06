#!/usr/bin/env python3
"""
CLI Tool for Generating Massive Weather Datasets
Supports generating billions of records for big data projects.
"""
import argparse
import sys
from pathlib import Path
from datetime import datetime, timedelta, timezone
import sys
from pathlib import Path

# Add project root and src to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / "src"))

from src.data_generator import (
    generate_millions_of_records,
    get_big_data_presets
)
from src.config import settings
import time

def format_number(num: int) -> str:
    """Format large numbers with K, M, B suffixes."""
    if num >= 1_000_000_000:
        return f"{num/1_000_000_000:.2f}B"
    elif num >= 1_000_000:
        return f"{num/1_000_000:.2f}M"
    elif num >= 1_000:
        return f"{num/1_000:.2f}K"
    return str(num)

def main():
    parser = argparse.ArgumentParser(
        description="Generate massive weather datasets for big data projects",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Generate 10 million records using preset
  python3 generate_big_data_cli.py --preset "Medium (10M records)"
  
  # Generate custom dataset: 5000 cities, 2 years, hourly
  python3 generate_big_data_cli.py --cities 5000 --days 730 --frequency hourly
  
  # Generate 1 billion records
  python3 generate_big_data_cli.py --cities 20000 --days 1825 --frequency hourly
  
  # List available presets
  python3 generate_big_data_cli.py --list-presets
        """
    )
    
    parser.add_argument(
        "--preset",
        type=str,
        help="Use a preset configuration (see --list-presets)"
    )
    
    parser.add_argument(
        "--cities",
        type=int,
        default=1000,
        help="Number of cities to generate data for (default: 1000)"
    )
    
    parser.add_argument(
        "--days",
        type=int,
        default=365,
        help="Number of days of historical data (default: 365)"
    )
    
    parser.add_argument(
        "--frequency",
        type=str,
        choices=["hourly", "daily"],
        default="hourly",
        help="Data frequency: hourly or daily (default: hourly)"
    )
    
    parser.add_argument(
        "--output-dir",
        type=str,
        default=None,
        help="Output directory (default: data/processed/big_data)"
    )
    
    parser.add_argument(
        "--list-presets",
        action="store_true",
        help="List available preset configurations"
    )
    
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Suppress progress output"
    )
    
    args = parser.parse_args()
    
    # List presets if requested
    if args.list_presets:
        print("\n📋 Available Presets:\n")
        presets = get_big_data_presets()
        for name, config in presets.items():
            if args.frequency == "hourly":
                records = config['num_cities'] * config['days'] * 24
            else:
                records = config['num_cities'] * config['days']
            
            print(f"  {name}:")
            print(f"    Cities: {config['num_cities']:,}")
            print(f"    Days: {config['days']:,}")
            print(f"    Frequency: {config['frequency']}")
            print(f"    Expected Records: {records:,} ({format_number(records)})")
            print(f"    Description: {config['description']}")
            print()
        return
    
    # Use preset if specified
    if args.preset:
        presets = get_big_data_presets()
        if args.preset not in presets:
            print(f"❌ Error: Preset '{args.preset}' not found.")
            print(f"Available presets: {', '.join(presets.keys())}")
            print("Use --list-presets to see all options.")
            sys.exit(1)
        
        config = presets[args.preset]
        num_cities = config['num_cities']
        days = config['days']
        frequency = config['frequency']
        print(f"📋 Using preset: {args.preset}")
        print(f"   {config['description']}")
    else:
        num_cities = args.cities
        days = args.days
        frequency = args.frequency
    
    # Calculate expected records
    if frequency == "hourly":
        expected_records = num_cities * days * 24
    else:
        expected_records = num_cities * days
    
    # Setup output directory
    if args.output_dir:
        output_dir = Path(args.output_dir)
    else:
        output_dir = settings.processed_data_dir / "big_data"
    
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Display configuration
    print("\n" + "="*70)
    print("🌐 BIG DATA GENERATOR - Configuration")
    print("="*70)
    print(f"  Cities:           {num_cities:,}")
    print(f"  Days:             {days:,} ({days/365:.1f} years)")
    print(f"  Frequency:        {frequency}")
    print(f"  Expected Records:  {expected_records:,} ({format_number(expected_records)})")
    print(f"  Output Directory: {output_dir}")
    
    # Estimate size and time
    estimated_size_gb = (expected_records * 200) / (1024**3)
    estimated_time_min = max(1, expected_records / 1_000_000)
    
    print(f"  Estimated Size:   {estimated_size_gb:.2f} GB")
    print(f"  Estimated Time:   ~{estimated_time_min:.0f} minutes")
    print("="*70 + "\n")
    
    # Confirm if very large
    if expected_records > 100_000_000:
        print("⚠️  WARNING: This will generate a VERY LARGE dataset!")
        print(f"   Expected: {expected_records:,} records ({estimated_size_gb:.2f} GB)")
        response = input("   Continue? (yes/no): ")
        if response.lower() not in ['yes', 'y']:
            print("❌ Cancelled.")
            sys.exit(0)
        print()
    
    # Progress callback
    start_time = time.time()
    last_update = 0
    
    def progress_callback(current: int, total: int):
        nonlocal last_update
        if args.quiet:
            return
        
        # Update every 100k records or every 5 seconds
        now = time.time()
        if current - last_update >= 100000 or now - start_time >= 5:
            progress = (current / total * 100) if total > 0 else 0
            elapsed = now - start_time
            rate = current / elapsed if elapsed > 0 else 0
            remaining = (total - current) / rate if rate > 0 else 0
            
            print(f"\r🔄 Progress: {current:,} / {total:,} ({progress:.1f}%) | "
                  f"Rate: {rate:,.0f} rec/s | "
                  f"ETA: {remaining/60:.1f} min", end="", flush=True)
            last_update = current
    
    # Generate data
    print("🚀 Starting data generation...\n")
    start_time = time.time()
    
    try:
        total_records, saved_files = generate_millions_of_records(
            num_cities=num_cities,
            days=days,
            frequency=frequency,
            output_dir=output_dir,
            progress_callback=progress_callback if not args.quiet else None
        )
        
        elapsed_time = time.time() - start_time
        
        # Calculate actual file sizes
        total_size_bytes = sum(f.stat().st_size for f in saved_files)
        total_size_mb = total_size_bytes / (1024 * 1024)
        total_size_gb = total_size_mb / 1024
        
        print("\n" + "="*70)
        print("✅ GENERATION COMPLETE!")
        print("="*70)
        print(f"  Total Records:    {total_records:,} ({format_number(total_records)})")
        print(f"  Files Created:    {len(saved_files):,}")
        print(f"  Total Size:       {total_size_gb:.2f} GB ({total_size_mb:.2f} MB)")
        print(f"  Time Elapsed:     {elapsed_time/60:.2f} minutes ({elapsed_time:.1f} seconds)")
        print(f"  Generation Rate:  {total_records/elapsed_time:,.0f} records/second")
        print(f"  Output Location:  {output_dir}")
        print("="*70)
        
        # Show sample files
        if len(saved_files) <= 10:
            print("\n📁 Generated Files:")
            for i, filepath in enumerate(saved_files, 1):
                file_size_mb = filepath.stat().st_size / (1024 * 1024)
                print(f"  {i}. {filepath.name} ({file_size_mb:.2f} MB)")
        else:
            print(f"\n📁 Generated {len(saved_files):,} files")
            print("   Sample files:")
            for i, filepath in enumerate(saved_files[:5], 1):
                file_size_mb = filepath.stat().st_size / (1024 * 1024)
                print(f"  {i}. {filepath.name} ({file_size_mb:.2f} MB)")
            print(f"  ... and {len(saved_files) - 5} more files")
        
        print("\n💡 Tip: Use the Streamlit app or 'View Data' page to explore your data!")
        print()
        
    except KeyboardInterrupt:
        print("\n\n❌ Generation interrupted by user.")
        sys.exit(1)
    except Exception as e:
        print(f"\n\n❌ Error during generation: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()

