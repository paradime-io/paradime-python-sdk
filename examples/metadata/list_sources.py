#!/usr/bin/env python3
"""
List Available Sources

This helper script lists all available sources in your schedule
to help you find the correct source_name and table_name for other examples.
"""
import os

from paradime.client.paradime_client import Paradime


def main() -> None:
    print("📋 List Available Sources")
    print("=" * 27)

    # Initialize client
    client = Paradime(
        api_key=os.getenv("PARADIME_API_KEY") or "",
        api_secret=os.getenv("PARADIME_API_SECRET") or "",
        api_endpoint=os.getenv("PARADIME_API_ENDPOINT") or "",
    )

    try:
        # Change this to your schedule name
        schedule_name = "snowflake-costs-analytics"

        print(f"\n🔍 Listing sources for: {schedule_name}")

        # Get all sources
        sources = client.metadata.get_source_freshness(schedule_name)

        if not sources:
            print("\n❌ No sources found")
            return

        print(f"\n📊 Found {len(sources)} sources:")
        print()

        # Group sources by source_name to show tables within each source
        sources_by_name: dict[str, list] = {}
        for source in sources:
            if source.source_name not in sources_by_name:
                sources_by_name[source.source_name] = []
            sources_by_name[source.source_name].append(source)

        # Display sources and their tables
        for source_name, source_tables in sorted(sources_by_name.items()):
            print(f"📁 Source: {source_name}")
            unique_count = len(set(s.name for s in source_tables))
            print(f"   Tables ({unique_count}):")

            # Remove duplicates by creating a set of unique table names
            unique_tables = {}
            for source in source_tables:
                unique_tables[source.name] = source

            for table_name, source in sorted(unique_tables.items()):
                status_value = (
                    source.freshness_status.value
                    if hasattr(source.freshness_status, "value")
                    else source.freshness_status
                )
                status_icon = (
                    "✅" if status_value == "pass" else "❌" if status_value == "error" else "⚠️"
                )
                age = f" ({source.hours_since_load:.1f}h)" if source.hours_since_load else ""
                print(f"     {status_icon} {source.name}{age}")
                print(f"        Database: {source.database}")
                print(f"        Schema: {source.schema_name}")
                print(f"        Status: {status_value}")
                if source.description:
                    desc = (
                        source.description[:60] + "..."
                        if len(source.description) > 60
                        else source.description
                    )
                    print(f"        Description: {desc}")
                print()
            print()

        # Show example usage
        if sources:
            example_source = sources[0]
            print("💡 Example usage in single_source_status.py:")
            print(f'   schedule_name = "{schedule_name}"')
            print(f'   source_name = "{example_source.source_name}"  # Source name')
            print(f'   table_name = "{example_source.name}"   # Table name within the source')

        print("\n✅ Source listing complete!")

    except Exception as e:
        print(f"❌ Error: {e}")
        print("\nMake sure to:")
        print("• Set your API credentials")
        print("• Update the schedule_name variable")

    finally:
        client.metadata.close()


if __name__ == "__main__":
    main()
