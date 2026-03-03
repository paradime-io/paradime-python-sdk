#!/usr/bin/env python3
"""
Single Source Status Example

This example shows how to fetch the status of a specific source:
- Get source freshness status
- Check last loaded time
- View source details
"""
import os

from paradime.client.paradime_client import Paradime


def main() -> None:
    print("🕒 Single Source Status Check")
    print("=" * 31)

    # Initialize client
    client = Paradime(
        api_key=os.getenv("PARADIME_API_KEY") or "",
        api_secret=os.getenv("PARADIME_API_SECRET") or "",
        api_endpoint=os.getenv("PARADIME_API_ENDPOINT") or "",
    )

    try:
        # Change these to your schedule and source name
        schedule_name = "daily prod run"
        source_name = "dlt_hubspot"  # Change to your source name

        print(f"\n🔍 Checking source: {source_name}")
        print(f"   Schedule: {schedule_name}")

        # Get all sources
        sources = client.metadata.get_source_freshness(schedule_name)

        # Find all tables in the specific source
        source_tables = []
        for source in sources:
            if source.name == source_name:  # Find all tables in this source
                source_tables.append(source)

        if not source_tables:
            print(f"\n❌ Source '{source_name}' not found")

            # Show all available source names
            unique_sources = set()
            for source in sources:
                unique_sources.add(source.name)

            print("\nAvailable source names:")
            for src_name in sorted(unique_sources):
                print(f"  📁 {src_name}")

            # Show matching sources for partial matches
            matching_sources = [
                s
                for s in sources
                if source_name.lower() in s.name.lower() or s.name.lower() in source_name.lower()
            ]
            if matching_sources:
                print("\n🔍 Closest matching sources:")
                for source in matching_sources[:5]:
                    status_icon = (
                        "✅"
                        if source.freshness_status == "pass"
                        else "❌" if source.freshness_status == "error" else "⚠️"
                    )
                    print(f"  {status_icon} {source.name}")
            else:
                print("\nFirst 10 available sources:")
                for source in sources[:10]:
                    status_icon = (
                        "✅"
                        if source.freshness_status == "pass"
                        else "❌" if source.freshness_status == "error" else "⚠️"
                    )
                    print(f"  {status_icon} {source.name}")

            print("\n💡 Tip: Run 'python list_sources.py' to see all available sources")
            return

        # Display source overview
        print(f"\n📊 Source: {source_name}")
        print(f"   Tables found: {len(source_tables)}")
        print()

        # Display each table in the source
        for table in sorted(source_tables, key=lambda x: x.identifier or ""):
            print(f"📋 Table: {table.identifier}")
            print(f"   Database: {table.database}")
            print(f"   Schema: {table.schema_name}")

            # Status
            status_value = (
                table.freshness_status.value
                if hasattr(table.freshness_status, "value")
                else table.freshness_status
            )
            if status_value == "pass":
                status_display = "✅ Fresh"
            elif status_value == "warn":
                status_display = "⚠️ Warning"
            elif status_value == "error":
                status_display = "❌ Stale"
            else:
                status_display = f"❓ {status_value}"

            print(f"   Status: {status_display}")

            # Timing information
            if table.max_loaded_at:
                print(f"   Last loaded: {table.max_loaded_at}")
                hours_old = table.hours_since_load or 0
                print(f"   Age: {hours_old:.1f} hours")
            else:
                print("   Last loaded: Unknown")

            # Thresholds
            if table.warn_after_hours:
                print(f"   Warning threshold: {table.warn_after_hours} hours")
            if table.error_after_hours:
                print(f"   Error threshold: {table.error_after_hours} hours")

            # Additional metadata
            if table.loader:
                print(f"   Loader: {table.loader}")
            if table.description:
                desc = (
                    table.description[:80] + "..."
                    if len(table.description) > 80
                    else table.description
                )
                print(f"   Description: {desc}")

            print()

        # Summary statistics
        fresh_count = sum(
            1
            for t in source_tables
            if (
                t.freshness_status.value
                if hasattr(t.freshness_status, "value")
                else t.freshness_status
            )
            == "pass"
        )
        stale_count = sum(
            1
            for t in source_tables
            if (
                t.freshness_status.value
                if hasattr(t.freshness_status, "value")
                else t.freshness_status
            )
            == "error"
        )
        warn_count = sum(
            1
            for t in source_tables
            if (
                t.freshness_status.value
                if hasattr(t.freshness_status, "value")
                else t.freshness_status
            )
            == "warn"
        )

        print(f"📊 Summary for {source_name}:")
        print(f"   ✅ Fresh: {fresh_count}")
        print(f"   ⚠️ Warning: {warn_count}")
        print(f"   ❌ Stale: {stale_count}")
        print(f"   📋 Total tables: {len(source_tables)}")

        print("\n✅ Source check complete!")

    except Exception as e:
        print(f"❌ Error: {e}")
        print("\nMake sure to:")
        print("• Set your API credentials")
        print("• Update the schedule_name, source_name, and table_name variables")

    finally:
        client.metadata.close()


if __name__ == "__main__":
    main()
