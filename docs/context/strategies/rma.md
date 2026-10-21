# RMAStrategy

Aggiornamento: 21 ottobre 2026. **Primo ciclo completato con esito NEGATIVE_RESULT:** nessuno dei nove challenger supera simultaneamente i criteri congelati di CAGR e drawdown. Nessuna modifica operativa o promozione.

Documento del ciclo principale: [report finale](../../../bt-strategy-test/RMA/study_20261020/reports/FINAL_REPORT.md). [Indice e riproduzione](../../../bt-strategy-test/RMA/study_20261020/README.md); [protocollo preregistrato](../../../bt-strategy-test/RMA/study_20261020/decisions/P2_P3_PRECOMMITTED_DESIGN.md). Il precedente [assessment e piano](../../../bt-strategy-test/RMA/assessment_20261020/README.md) conserva il punto di partenza.

Oggetto: `weekly.RMAStrategy`, su barre **daily** nello snapshot studiato. `daily.DailyRMAStrategy` è un alias; `daily.RMAStrategy` è diversa. RMA indica OLS sul log-prezzo, non la media di Wilder: periodi 180/30, ranking slope180 × R², filtro veloce sul segno della slope30. Warm-up effettivo 359 barre. `min_concurrent` è ampiezza della selezione, non massimo permanente di posizioni. Uscite da segnale e trailing nativi esentano i titoli ancora selezionati; il filtro di volatilità restringe anche la lista dei segnali di uscita.

Il confronto economico usa 100 titoli correnti esclusa KDP, Yahoo raw, warm-up dal 2008, portafoglio continuo dal 2010, selezione 2015–2022, capitale 20.000 e fill tutti next-open. Frizione proporzionale 2,5 bps per lato, addebitata nel broker; nessuno slippage aggiuntivo. Baseline: CAGR 20,83%, maxDD −41,02%. Ampiezza 5: 18,69% e −32,94%; ampiezza 10: 14,53% e −25,80%. La diversificazione riduce il rischio con una perdita di CAGR superiore al massimo ammesso di 2 punti. C03 manca di poco entrambi i limiti, ma resta respinto. Il regime QQQ e le alternative EMA/SMA/HMA/KAMA al filtro veloce non superano i gate; non è stata testata una sostituzione completa delle regressioni.

L'accelerazione positiva estrema mostra rischio aggregato maggiore, ma non stabile abbastanza tra anni; il suo intervallo bootstrap include zero. Anche il contrasto slope10−slope30 e l'estensione falliscono i criteri P2. **Nessun supporto sufficiente per aggiungere la terza regressione o i tre filtri specifici.** Non è una prova che ogni possibile accelerazione sia inutile.

Le prove tecniche passano: 52 test implementativi, 12 test del comparatore, verifica indipendente dei ledger e ricostruzione delle medie. Nessun finalista: nessun challenger sbloccato sul 2023–2025 e nessuno stress 2/10 bps, riservato al finalista. P1 resta una replica tecnica separata sui 101 titoli fino al 2026, con uscite same-close e dati problematici documentati; i suoi numeri non sono direttamente confrontabili con questa matrice.

Limiti: universo corrente retrospettivo, assenza di membership point-in-time e contabilità completa dei dividendi/corporate action; storico già osservato, nessun holdout intonso. Prezzi e rendimenti sono proxy di ricerca, non certificazione live. Prima di ulteriori ottimizzazioni, migliorare la qualità dell'evidenza e preregistrare un nuovo compromesso tra rendimento e rischio; non allargare retroattivamente i gate.

## Estensione successiva: solo overnight

Il nuovo ciclo richiesto dall’utente è concluso con **EXPLORATORY_SUPPORT**, senza modificare l’esito negativo dei nove challenger precedenti. [Report AH completo](../../../bt-strategy-test/RMA/session_overnight_20261021/reports/FINAL_REPORT.md); [protocollo e riproduzione](../../../bt-strategy-test/RMA/session_overnight_20261021/README.md).

Usa il ranking nativo 180/30 della seduta precedente, primi due titoli, 40% del cash noto al mattino per slot, quantità da Close precedente e cap SMA20 volume; compra al Close della sera successiva e vende al successivo Open. Il segnale ritardato evita decisioni basate sulla medesima chiusura del fill. Costi 2/5/10 bps round-trip e replica R0_5 sul 2010–2025 sono stati congelati prima dei risultati.

A 5 bps, nel 2015–2022 AH dà CAGR 23,76% e DD −26,50%, contro 20,83% e −41,02% di R0. Sul 2010–2025 AH rende 22,24% contro 27,15% di R0, conservando il DD inferiore. A 10 bps AH scende al 10,59% di CAGR completo, DD −35,19%: forte sensibilità al turnover. Tutti i quattro flag esplorativi passano, incluso l’intervallo bootstrap del differenziale quotidiano contro QQQ overnight, ma non è una promozione. Il 41,34% del PnL netto AH5 è attribuito ad APP. Universo corrente, dividendi mancanti e proxy delle aste restano limiti materiali.

Questa è una nuova strategia di sessione della famiglia RMA, con selezione quotidiana e liquidazione obbligatoria, non una prova controllata che eliminare soltanto la componente RTH del portafoglio originario migliori tutto lo storico. Il 2023–2025 è descrittivo e già noto. Produzione e profili broker restano invariati.
