#!/usr/bin/env python3
"""
Simple Performance Analysis Example

This example shows how to analyze model performance:
- Model execution times
- Status tracking
- Bottleneck identification
"""
import os

from paradime.client.paradime_client import Paradime


def main() -> None:
    print("⚡ Simple Performance Analysis")
    print("=" * 31)

    # Initialize client
    client = Paradime(
        api_key=os.getenv("PARADIME_API_KEY") or "",
        api_secret=os.getenv("PARADIME_API_SECRET") or "",
        api_endpoint=os.getenv("PARADIME_API_ENDPOINT") or "",
    )

    try:
        # Change this to your schedule name
        schedule_name = "daily prod run"
        print(f"\n⚡ Analyzing: {schedule_name}")

        # Get models
        models = client.metadata.get_model_health(schedule_name)
        print(f"\n📊 Total Models: {len(models)}")

        # 1. Execution time analysis
        print("\n⏱️ Execution Time Analysis:")

        models_with_time = [m for m in models if m.execution_time is not None]
        if models_with_time:
            total_time = sum(
                m.execution_time for m in models_with_time if m.execution_time is not None
            )
            avg_time = total_time / len(models_with_time)

            print(f"  Models with timing: {len(models_with_time)}")
            print(f"  Total execution time: {total_time:.1f}s ({total_time/60:.1f}m)")
            print(f"  Average time: {avg_time:.1f}s")

            # Show slowest models
            slowest = sorted(models_with_time, key=lambda x: x.execution_time or 0, reverse=True)[
                :5
            ]
            print("\n  Slowest models:")
            for model in slowest:
                print(
                    f"    • {model.name}: {model.execution_time:.1f}s ({model.materialized_type})"
                )
        else:
            print("  No timing data available")

        # 2. Status analysis
        print("\n📋 Status Distribution:")
        status_counts: dict[str, int] = {}
        for model in models:
            status = str(model.status)
            status_counts[status] = status_counts.get(status, 0) + 1

        for status, count in status_counts.items():
            percentage = (count / len(models)) * 100
            print(f"  {status}: {count} ({percentage:.1f}%)")

        # 3. Performance by materialization
        print("\n🏗️ Performance by Materialization:")
        mat_performance: dict[str, list[float]] = {}

        for model in models_with_time:
            mat_type = model.materialized_type or "unknown"
            if mat_type not in mat_performance:
                mat_performance[mat_type] = []
            if model.execution_time is not None:
                mat_performance[mat_type].append(model.execution_time)

        for mat_type, times in mat_performance.items():
            avg_time = sum(times) / len(times)
            max_time = max(times)
            print(f"  {mat_type}: avg {avg_time:.1f}s, max {max_time:.1f}s ({len(times)} models)")

        # 4. Find optimization opportunities
        print("\n🎯 Optimization Opportunities:")

        # Long-running models
        slow_models = [
            m for m in models_with_time if m.execution_time is not None and m.execution_time > 300
        ]  # 5+ minutes
        if slow_models:
            print(f"  Slow models (5+ minutes): {len(slow_models)}")
            print("    Consider incremental materialization or performance tuning")

        # Models with many dependencies
        complex_models = [m for m in models if len(m.depends_on or []) > 8]
        if complex_models:
            print(f"  High dependency models: {len(complex_models)}")
            print("    Consider breaking down complex models")

        # Models without timing data
        no_timing = len(models) - len(models_with_time)
        if no_timing > len(models) * 0.1:  # >10% missing timing
            print(f"  Missing timing data: {no_timing} models")
            print("    Ensure run_results.json captures complete timing")

        # 5. Simple recommendations
        print("\n💡 Recommendations:")
        if models_with_time and total_time > 3600:  # > 1 hour
            print("  • Long build time detected - consider incremental strategies")
        if models_with_time and avg_time > 120:  # > 2 minutes average
            print("  • High average execution time - focus on SQL optimization")
        if slow_models:
            print("  • Review slow models for performance improvements")

        print("\n✅ Analysis complete!")

    except Exception as e:
        print(f"❌ Error: {e}")
        print("\nMake sure to set your API credentials and check the schedule name.")

    finally:
        client.metadata.close()


if __name__ == "__main__":
    main()
