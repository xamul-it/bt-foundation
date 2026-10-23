# Vincoli di esecuzione e leva verificati

Verifica documentale del22ottobre2026, senza accesso al conto o a credenziali. Non è un audit delle abilitazioni effettive dell'utente. Le fonti sono esterne al freeze dei backtest.

La documentazione ufficiale di [Alpaca sui margini](https://docs.alpaca.markets/us/docs/margin-and-short-selling) descrive per azioni marginabili il limite overnight Reg T normalmente2x, initial margin minimo50% e maintenance variabile per strumento. La tabella pubblicata indica30% per long sopraUSD6, percentuali maggiori per titoli a basso prezzo e ETF a leva; esistono anche maggiorazioni per concentrazione. Il broker locale congelato usa maintenance0,25: **i due modelli non coincidono**. Un run senza liquidazioni nel modello locale non certifica il rispetto del margine reale.

La pagina ufficiale sulle [negoziazioni24/5](https://docs.alpaca.markets/us/docs/245-trading-for-trading-api) conferma che il buying power intraday non si estende alla sessione overnight. AH in questo studio mantiene Close→Open: non confondere questo rischio overnight con una strategia che chiude prima della fine del giorno.

Il finanziamento10%annuo è la convenzione comune dello studio, non il tasso contrattuale del conto. La stessa [documentazione margini](https://docs.alpaca.markets/us/docs/margin-and-short-selling) rinvia al tariffario aggiornato per il tasso applicabile e addebita sul debito overnight. Il broker di ricerca addebita debito incrementale secondo la propria convenzione giorni/360: formula, arrotondamenti e trattamento delle festività restano da confrontare col contratto reale. Nessuna tariffa personale è stata letta o presunta.

Le [regole ufficiali degli ordini](https://docs.alpaca.markets/us/docs/orders-at-alpaca) distinguono istruzioni d'asta e finestre di accettazione. I fill a Open/Close daily di Backtrader restano proxy; non provano partecipazione, priorità, quantità eseguibile, costo d'asta o equivalenza con un ingresso anticipato. Il confronto prospettico deve usare le stesse gambe temporali.

La disponibilità dell'utente a impiegare tutto il capitale di rischio non risolve questi vincoli. Il test2x è informativo sulla policy nativa e sul costo convenzionale;10x non è stato simulato né estrapolato. Usare altri veicoli richiederebbe un diverso studio economico e di esecuzione.
