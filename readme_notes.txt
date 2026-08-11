878 messages traités en 4 min (6 capteurs, --fault-rate 0.2)
  805 valides -> Parquet/MinIO, partitionné site_id/date
   73 en quarantaine avec motif -> range 44, completeness 22, plausibility 7
   22 rejetés en amont par la DLQ du bridge (structure invalide)
    0 valeur hors bornes, 0 doublon dans le flux propre
    6 documents d'état courant dans MongoDB
Coupure du bridge 30 s sous charge : 0 message perdu (offsets 660 -> 756)