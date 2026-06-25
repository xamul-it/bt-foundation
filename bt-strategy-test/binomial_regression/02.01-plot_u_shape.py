#!/usr/bin/env python3
"""Visualizza il pattern a U tra slope e outcome futuro."""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path

# Carica dati slice analysis
df = pd.read_csv("results/diagnostics/feature_slice_analysis.csv")

# Filtra solo slope_60 vs high/low
high_df = df[(df['feature'] == 'slope_60') & (df['outcome'] == 'high_ret_max_5m')].copy()
low_df = df[(df['feature'] == 'slope_60') & (df['outcome'] == 'low_ret_min_5m')].copy()

# Calcola punto medio di ogni slice
high_df['slope_mid'] = (high_df['slice_low'] + high_df['slice_high']) / 2
low_df['slope_mid'] = (low_df['slice_low'] + low_df['slice_high']) / 2

# Plot
fig, axes = plt.subplots(2, 1, figsize=(12, 10))

# High
ax = axes[0]
ax.plot(high_df['slope_mid'], high_df['mean_outcome_in_slice'] * 100,
        marker='o', markersize=10, linewidth=2, color='#2E7D32', label='High ret max 5m')
ax.axhline(0, color='black', linestyle='--', alpha=0.3)
ax.axvline(0, color='black', linestyle='--', alpha=0.3)
ax.set_xlabel('Slope_60 (punto medio slice)', fontsize=12)
ax.set_ylabel('Mean high_ret_max_5m (%)', fontsize=12)
ax.set_title('Pattern a U: Slope vs High Futuro (5 minuti)', fontsize=14, fontweight='bold')
ax.grid(alpha=0.3)
ax.legend()

# Annota i punti estremi
min_idx = high_df['mean_outcome_in_slice'].idxmin()
max_idx = high_df['mean_outcome_in_slice'].idxmax()
ax.annotate(f"Min: {high_df.loc[min_idx, 'mean_outcome_in_slice']*100:.2f}%\n(slope ~0)",
            xy=(high_df.loc[min_idx, 'slope_mid'], high_df.loc[min_idx, 'mean_outcome_in_slice']*100),
            xytext=(high_df.loc[min_idx, 'slope_mid'], high_df.loc[min_idx, 'mean_outcome_in_slice']*100 - 0.05),
            fontsize=10, ha='center',
            bbox=dict(boxstyle='round', facecolor='yellow', alpha=0.7),
            arrowprops=dict(arrowstyle='->', color='red'))

# Low
ax = axes[1]
ax.plot(low_df['slope_mid'], low_df['mean_outcome_in_slice'] * 100,
        marker='s', markersize=10, linewidth=2, color='#B71C1C', label='Low ret min 5m')
ax.axhline(0, color='black', linestyle='--', alpha=0.3)
ax.axvline(0, color='black', linestyle='--', alpha=0.3)
ax.set_xlabel('Slope_60 (punto medio slice)', fontsize=12)
ax.set_ylabel('Mean low_ret_min_5m (%)', fontsize=12)
ax.set_title('Pattern a U: Slope vs Low Futuro (5 minuti)', fontsize=14, fontweight='bold')
ax.grid(alpha=0.3)
ax.legend()

# Annota
max_idx_low = low_df['mean_outcome_in_slice'].idxmax()  # Più alto (meno negativo)
ax.annotate(f"Max: {low_df.loc[max_idx_low, 'mean_outcome_in_slice']*100:.2f}%\n(slope ~0, meno drawdown)",
            xy=(low_df.loc[max_idx_low, 'slope_mid'], low_df.loc[max_idx_low, 'mean_outcome_in_slice']*100),
            xytext=(low_df.loc[max_idx_low, 'slope_mid'], low_df.loc[max_idx_low, 'mean_outcome_in_slice']*100 + 0.05),
            fontsize=10, ha='center',
            bbox=dict(boxstyle='round', facecolor='lightgreen', alpha=0.7),
            arrowprops=dict(arrowstyle='->', color='green'))

fig.tight_layout()
fig.savefig("results/diagnostics/u_shape_pattern.png", dpi=150)
print(f"✅ Grafico salvato in: results/diagnostics/u_shape_pattern.png")
plt.show()
