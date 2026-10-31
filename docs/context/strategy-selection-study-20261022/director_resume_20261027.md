# Ripresa del Research Director — 27 ottobre 2026

Mandato di ricerca ancora aperto; cutoff economico 2026-10-21. Nessun ordine, profilo, servizio, deploy o schedulazione operativa è autorizzato. Questa nota deriva da lettura attuale del prompt, protocollo dell'estensione v0.4, README, final_report, rettifica di adeguatezza, ledger e output, non da una certificazione del precedente riepilogo.

## Stato verificato e priorità decisionali

I 21 tentativi originali conservano risultati e gate; la loro correttezza non colma la mancanza di calibrazione economica rappresentativa o diagnosi residua. Nel ledger dell'estensione risultano due completamenti exit 0: E_AH0_RT5 e E_RM0_RT5, entrambi con extension_completion.completed=true. Un completamento tecnico non è audit economico né G4. Nessun risultato definitivo dell'estensione è ancora certificato.

| Questione | Evidenza necessaria a chiudere il punto | Priorità e limite |
| --- | --- | --- |
| Copertura 2000–2010, specialmente RMA | Tre baseline con date feed/readiness/primo ordine/fill, riconciliazione e volumi zero | Prima: FC0 manca; warmup teorico non prova attività economica dal gennaio 2000 |
| Famiglia/configurazione da osservare oggi | Confronto AH0, FC0, RM0 e pacchetti A01/C2 ai due costi, crescita/rischio/recupero e configurazioni complete | Prima: A01/C2 non sono confutati dai negativi di altri pacchetti; distinguere preferenza di ricerca e promozione operativa |
| Lungo contro recente | Menu rappresentativo 5×3×2, calendari causali, G3, 12 percorsi continui e 18 contrasti simultanei | Prima: ranking terminale o proxy concatenato non sostituiscono la verifica nativa |
| Deterioramento | Sessioni coerenti, esposizione da fill, ridge fissata 2005–2022, placebo 2010–2022, potere/ritardo, test 2023–cutoff | Prima: luglio è sottoperiodo scelto ex post; nessun dato identificante sul crowding |
| Switch, riduzione, rientro | Conservare esiti originali, confrontare proxy diagnostico con shadow e dichiarare limiti di stato/costi | Seconda: nessuna riapertura di CUSUM/rotazioni negative e nessuna promozione da proxy |
| Capitale e rischio | Scenari nativi originali con date esplicite più nuovi percorsi a 200k, costi, DD e recuperi | Seconda: il conto è tutto capitale di rischio; nessuna preferenza personale inventata |

## Valutazione del protocollo

Confermo il disegno v0.4 come risposta proporzionata alle lacune, incluso il tetto di 49 tentativi (30 statici, 12 procedure, 4 pacchetti, 3 pilot tecnici). I tre pilot di fedeltà sono giustificati dalle tre famiglie e non introducono una nuova selezione economica. Le decisioni negative precedenti restano ferme. Non occorre consumare il tetto; non si ampliano menu/gate dopo esiti deludenti.

Le debolezze da riportare vicino alle conclusioni sono: universi/pesi selezionati ex post, unità ADV non certificate, compounding e cap nei ritorni shadow, provider/universi diversi fra famiglie, assenza di holdout intatto. Un mancato PASS non prova equivalenza né ottimalità del riferimento. Una diagnosi fattoriale è condizionata e non identifica causalmente l'edge.

G3 deve verificare readiness originaria per ciascun simbolo e stato continuo, inclusi primi ingressi/IPO, trailing e ordini pendenti. G4 deve coprire adeguatezza della risposta oltre all'aritmetica. Il minimo per dichiarare concluso il mandato è una proposta condizionata con tutte le righe sopra realmente valutate oppure una causa di impossibilità dimostrata per le prove mancanti: la sola scrittura del protocollo, un budget terminato o una dichiarazione generica di incertezza non bastano.

## Piano diagnostico e requisiti dati comunicati al coordinatore

Esiste già scripts/diagnose_deterioration.py e reviews/diagnostic_method.md, fissati prima degli esiti. Il nuovo script di ripresa riuserà quella specifica senza scegliere regressori, finestre o soglie dai risultati. Input: completion, resolved_plan, effective_strategy, equity.jsonl, orders.jsonl e barre frozen SPY/QQQ/roster; SHA-256 e cutoff espliciti. Output separati e senza sovrascrittura. Primo calcolo su AH0/RM0; FC0 e stress costi appena completati e verificati.

Controlli da rafforzare prima dell'interpretazione: eleggibilità dell'equal-weight determinata dal close precedente, nessuna rinormalizzazione sui simboli privi del prezzo futuro; hash di tutte le barre usate; NAV nonpositivo come perdita del conto; hedge separato dalla normale esposizione long; stato dell'allarme recente distinto da un allarme storico nel 2023–2026. L'intervallo bootstrap del train è un intervallo di riferimento predittivo condizionato, non un intervallo di confidenza dell'edge residuo. Gli anni placebo sono proxy di falsi allarmi, senza etichette di edge certamente sano.

La diagnostica può confrontare soltanto integrità dei replay. La qualità reale dell'esecuzione manca: non leggere credenziali né attivare connessioni broker per colmare questo limite. Nessun lancio economico compete a questo ruolo; usare un solo thread numerico e lasciare almeno due core liberi.

## Primo checkpoint eseguito nella ripresa

Nuovo `extension_20261023/scripts/diagnose_regimes_resume.py`, con output separati in `analysis/deterioration_resume_20261027/`. Le due baseline AH0/RM0 RT5 sono state analizzate: nessun allarme persistente AH; RMA presenta16date persistenti dal30settembre alcutoff. La debolezza AH da luglio resta un residuo negativo entro la dispersione di riferimento, non una dimostrazione che il mercato spieghi integralmente il calo. Il potere nei placebo è basso e non sostiene automatismi. Dettagli, intervalli e limiti sono nel `director_diagnostic_report.md` di quella directory; review indipendente richiesta, non ancora conclusa al momento di questo checkpoint. Selfcheck e4regressioni dedicate passano; nessun replay economico eseguito da questo ruolo.

Il coordinatore richiede poi FC0 e stressRT10, oltre ai pacchetti A01/C2 quando disponibili. Lo script riconosce i ruoli specifici hedge_moc_entry/hedge_moo_exit e conserva hash/snapshot per le versioni eseguite. La conclusione sullo switch resta quella delle evidenze già prodotte: nessun candidato automatico supportato, senza affermare impossibilità di ogni altra policy.

## Precisazione G3 condivisa con reviewer

Le baseline statiche originali non hanno archiviato tutti i vettori interni rlist/dlist/ranks. Non è possibile inventare un confronto retrospettivo di quei segnali. I tre replay tecnici CONST devono uguagliare submissions/fill/NAV/posizioni/readiness su tutte le date, e integrare fixture purecalc indipendenti di ranking/indicatori originali contro bank/dispatch, più prenext/IPO/cache/stato. Questa è la prova di equivalenza comportamentale e dell'implementazione equivalente, con limite esplicito sugli interni non archiviati. Il reviewer concorda sul limite; il gate tecnico consente di raccogliere i tre pilot ma G3 resta chiuso fino al confronto effettivo. Nessun quarto pilot né modifica del protocollo dopo gli esiti.
