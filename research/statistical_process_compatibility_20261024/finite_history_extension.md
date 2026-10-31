# Estensione dichiarata prima di eseguirla: capacità dello storico

Il confronto principale usa storici lunghi per isolare i difetti dei metodi.
Non è un'ipotesi realistica che una singola strategia abbia centinaia di anni di
sessioni indipendenti. Per non confondere una dimostrazione col fabbisogno reale,
si aggiunge questo esperimento successivo, distinto dal protocollo principale.

Seed 20261025. Baseline sticky rho=0,6; training 1.000; gap 100 fra segmenti;
calibrazioni A/B con 16, 47 o 100 blocchi ciascuna, passo 40 e lunghezza 20.
Storie totali: 2.480, 4.960, 9.200 osservazioni. L'equivalenza in anni usa solo
la convenzione illustrativa di 252 sessioni/anno, senza assumere dati reali.
Per ogni dimensione: 20 baseline indipendenti; 2.000 episodi per ciascuno degli
scenari sticky invariato, iid e sticky rho=0,9. Score ensemble a 20 lanci e
aggregato sulle cinque finestre; nessuna modifica a score/soglie primari.
Riportare distribuzione fra baseline, risoluzione minima 1/(B+1), falsi allarmi
e potenza. Il caso B=16 ha p minimo 1/17 > 5%: non può generare allarmi al 5%
con un rango non randomizzato, qualunque sia la severità dell'evento recente.
