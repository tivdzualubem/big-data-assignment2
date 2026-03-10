import matplotlib.pyplot as plt
import numpy as np

# Benchmark means from your results
labels = ["Q1", "Q2", "Q3"]

postgres = [1.961, 0.121, 0.0747]
mongodb = [20.150, 2.492, 0.7562]
neo4j = [15.663, 1.492, 1.370]

x = np.arange(len(labels))
width = 0.24

fig, ax = plt.subplots(figsize=(9, 5.5))

bars1 = ax.bar(x - width, postgres, width, label="PostgreSQL")
bars2 = ax.bar(x, mongodb, width, label="MongoDB")
bars3 = ax.bar(x + width, neo4j, width, label="Neo4j")

ax.set_xlabel("Queries")
ax.set_ylabel("Duration in seconds")
ax.set_title("Query execution results on average")
ax.set_xticks(x)
ax.set_xticklabels(labels)
ax.legend()

# Add values above bars
def add_labels(bars):
    for bar in bars:
        h = bar.get_height()
        ax.annotate(f"{h:.3f}" if h < 1 else f"{h:.3f}".rstrip('0').rstrip('.'),
                    xy=(bar.get_x() + bar.get_width() / 2, h),
                    xytext=(0, 3),
                    textcoords="offset points",
                    ha="center", va="bottom", fontsize=9)

add_labels(bars1)
add_labels(bars2)
add_labels(bars3)

plt.tight_layout()
plt.savefig("benchmark_chart.png", dpi=300, bbox_inches="tight")
print("Saved benchmark_chart.png")
