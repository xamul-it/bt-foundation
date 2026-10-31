// Shared by profile and run diagnostics. Temporal compatibility is tracked in bt-dash#11.
export default {
    compatibilityDelta(comparison) {
      const baseline = this.selectedBaseline?.metrics?.mean
      const recent = comparison?.recent?.metrics?.mean
      if (baseline === null || baseline === undefined || recent === null || recent === undefined) return null
      const delta = Number(recent) - Number(baseline)
      return Number.isFinite(delta) ? delta : null
    },
    compatibilityColor(comparison) {
      const delta = this.compatibilityDelta(comparison)
      // Tolerance keeps the inclusive ±0.01 boundary stable under float subtraction.
      if (delta === null || Math.abs(delta) <= 0.01 + 1e-12) return 'text-grey-7'
      return delta > 0 ? 'text-positive' : 'text-negative'
    },
    compatibilityDirectionText(comparison) {
      const delta = this.compatibilityDelta(comparison)
      if (delta === null) return 'Dati insufficienti per confrontare il gain medio con la baseline.'
      const direction = Math.abs(delta) <= 0.01 + 1e-12
        ? 'nella fascia neutra' : (delta > 0 ? 'superiore alla baseline' : 'inferiore alla baseline')
      return `Gain medio/trade ${direction}: scarto ${delta >= 0 ? '+' : ''}${delta.toFixed(6)} punti percentuali. Fascia neutra: ±0,01 punti percentuali. Il colore indica la direzione del gain; la percentuale indica la compatibilità statistica.`
    },
    compatibilityLabel(comparison) {
      if (this.compatibilityDelta(comparison) === null) return '—'
      const baselineMean = Number(this.selectedBaseline?.metrics?.mean)
      const baselineStddev = Number(this.selectedBaseline?.metrics?.stddev)
      const recentMean = Number(comparison?.recent?.metrics?.mean)
      const sampleSize = Number(comparison?.recent?.sample_size)
      if (![baselineMean, baselineStddev, recentMean, sampleSize].every(Number.isFinite) || baselineStddev <= 0 || sampleSize <= 0) return '—'
      const z = Math.abs(recentMean - baselineMean) / (baselineStddev / Math.sqrt(sampleSize))
      return this.pct(this.normalTwoSidedProbability(z))
    },
    normalTwoSidedProbability(z) {
      // Abramowitz-Stegun approximation of erfc(z / sqrt(2)).
      const t = 1 / (1 + 0.2316419 * z)
      const density = 0.3989422804014327 * Math.exp(-0.5 * z * z)
      const upperTail = density * ((((1.330274429 * t - 1.821255978) * t + 1.781477937) * t - 0.356563782) * t + 0.319381530) * t
      return Math.max(0, Math.min(1, 2 * upperTail))
    },
}
