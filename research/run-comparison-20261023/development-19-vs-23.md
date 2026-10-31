# Confronto development: 19 e 23 ottobre 2026

Verifica in sola lettura degli output originali e delle ricevute nei log dei job.

- Run 19/10 14:14:57: `c7e0f82e-e65a-4650-9d3a-aa60299ed509`.
- Run 23/10 18:28:15: `d6502e58-0fcc-460f-990b-20b20207167a`.
- Le configurazioni richieste nelle ricevute dei due job sono identiche, incluso l'intervallo 2023-10-19 / 2026-10-19 e capitale iniziale 200000.
- I commit registrati differiscono ed entrambi i run dichiarano codice dirty; il confronto non certifica identità di codice e dati.

| Metrica | Run 19 ottobre | Run 23 ottobre |
|---|---:|---:|
| Ultima seduta effettiva in returns.csv | 2026-10-16 | 2026-10-19 |
| Sedute | 751 | 752 |
| Trade chiusi | 2001 | 2004 |
| Rendimento totale % | 820.2831422254283 | 846.1940675906994 |
| Equity finale | 1840566.2844508565 | 1892388.1351813988 |

Ricalcolo con pandas sui returns.csv originali, senza simulazioni nuove:

- Equity del secondo run limitata alle date del primo: 1840566.2543878772.
- Differenza sul periodo comune: -0.0300629793.
- Rendimento della seduta aggiuntiva 19 ottobre: 2.81554009098989%.
- Contributo netto della seduta aggiuntiva: 51821.88079352165.
- Differenza totale equity: 51821.850730542326, pari a 25.9109253652711 punti percentuali sul capitale iniziale.
- Tutti i primi 2001 trade hanno identici asset, date apertura/chiusura e size. I PnL presentano piccoli scarti numerici (massimo assoluto 0.14386948 per trade); la causa dei piccoli scarti non è stata isolata.

Tre trade aggiuntivi, segnale/apertura registrata 16 ottobre e chiusura registrata 19 ottobre:

| Asset | Size | PnL lordo |
|---|---:|---:|
| NVDA | 2824 | 11211.283447265545 |
| AVGO | 1781 | 17810 |
| MU | 658 | 22839.154296875 |

Entrambi i positions.json sono vuoti. Nel primo run gli ordini del 16 ottobre per NVDA/AVGO/MU sono ancora Submitted: mancando la barra successiva non sono eseguiti. Non si tratta di tre posizioni aperte già contabilizzate nel primo run.

Fonti: `out/overnight_ah/OvernightAH/simulation_<id senza trattini>/{results.json,returns.csv,trades.json,orders.json,positions.json,today.json}` e `out/watchtower-calculations/<id>.log`.
