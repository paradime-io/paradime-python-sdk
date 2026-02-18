#!/usr/bin/env python3
"""
Simple Discovery Insights Example

This example shows how to explore your dbt project:
- Find documented vs undocumented models
- Explore model lineage and dependencies
- Get project overview
"""
import os

from paradime.client.paradime_client import Paradime


def main() -> None:
    print("🔍 Simple Discovery Insights")
    print("=" * 29)

    # Initialize client
    client = Paradime(
        api_key=os.getenv("PARADIME_API_KEY") or "",
        api_secret=os.getenv("PARADIME_API_SECRET") or "",
        api_endpoint=os.getenv("PARADIME_API_ENDPOINT") or "",
    )

    try:
        # Change this to your schedule name
        schedule_name = "daily prod run"
        print(f"\n🔍 Exploring: {schedule_name}")

        # Get data
        models = client.metadata.get_model_health(schedule_name)
        sources = client.metadata.get_source_freshness(schedule_name)

        # 1. Project overview
        print("\n📊 Project Overview:")
        print(f"  Models: {len(models)}")
        print(f"  Sources: {len(sources)}")

        # Show materialization breakdown
        materializations: dict[str, int] = {}
        for model in models:
            mat_type = model.materialized_type or "unknown"
            materializations[mat_type] = materializations.get(mat_type, 0) + 1

        print("\n  Materializations:")
        for mat_type, count in sorted(materializations.items()):
            print(f"    {mat_type}: {count}")

        # 2. Documentation coverage
        print("\n📚 Documentation Status:")
        documented = [m for m in models if m.description and len(m.description.strip()) > 10]
        doc_rate = (len(documented) / len(models) * 100) if models else 0
        print(f"  Documented: {len(documented)} ({doc_rate:.1f}%)")
        print(f"  Undocumented: {len(models) - len(documented)}")

        # Show a few undocumented models
        undocumented = [m for m in models if not m.description or len(m.description.strip()) <= 10]
        if undocumented:
            print("\n  Models needing documentation:")
            for model in undocumented[:3]:
                print(f"    • {model.name}")

        # 3. Model lineage insights
        print("\n🕸️ Lineage Overview:")

        # Find models with most dependencies
        models_with_deps = [m for m in models if m.depends_on]
        if models_with_deps:
            most_complex = sorted(
                models_with_deps, key=lambda x: len(x.depends_on or []), reverse=True
            )[:3]
            print("\n  Most connected models:")
            for model in most_complex:
                deps = len(model.depends_on or [])
                children = len(model.children_l1 or [])
                print(f"    • {model.name}: {deps} dependencies, {children} children")

        # Find root models (no dependencies)
        root_models = [m for m in models if not m.depends_on or len(m.depends_on) == 0]
        print(f"\n  Root models: {len(root_models)}")

        # Find leaf models (no children)
        leaf_models = [m for m in models if not m.children_l1 or len(m.children_l1) == 0]
        print(f"  Leaf models: {len(leaf_models)}")

        # 4. Analyze a specific model (if available)
        if models:
            focus_model = models[0]  # Take the first model as example
            print(f"\n🔍 Example Model Analysis: {focus_model.name}")
            print(f"  Status: {focus_model.status}")
            print(f"  Materialization: {focus_model.materialized_type}")
            print(f"  Tests: {focus_model.total_tests}")
            print(f"  Dependencies: {len(focus_model.depends_on or [])}")
            print(f"  Children: {len(focus_model.children_l1 or [])}")

        # 5. Simple recommendations
        print("\n💡 Recommendations:")
        if doc_rate < 70:
            print("  • Add descriptions to undocumented models")
        if len(root_models) == 0:
            print("  • Check if all models have proper source dependencies")
        if len(leaf_models) > len(models) * 0.5:
            print("  • Many leaf models found - consider if they're being used")

        print("\n✅ Discovery complete!")

    except Exception as e:
        print(f"❌ Error: {e}")
        print("\nMake sure to set your API credentials and check the schedule name.")

    finally:
        client.metadata.close()


if __name__ == "__main__":
    main()
