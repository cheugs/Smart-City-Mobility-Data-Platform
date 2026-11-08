# Performance et tests de charge

**Français** · [English](en/PERFORMANCE.md)

## Comment les mesures sont faites

`python scripts/run_benchmark.py N` :
1. arrête le producteur normal, pour ne mesurer que les événements du benchmark ;
2. envoie N relevés de trafic **valides** le plus vite possible (`producer/benchmark_load.py`), répartis sur les 60 capteurs ;
3. attend que les N relevés soient dans `raw.traffic_readings`, puis mesure ;
4. relève `docker stats` toutes les 5 secondes (CPU et mémoire de chaque conteneur) ;
5. redémarre le producteur et enregistre tout dans `benchmarks/benchmark_N.json`.

Les quatre requêtes Spark tournent pendant le benchmark : chaque relevé est donc lu deux fois
(table brute + fenêtres) et traverse tout le pipeline réel (validation, dédoublonnage, watermark, upserts).

**Machine :** ordinateur portable, Docker Desktop (WSL 2) avec 12 CPU et 8 Go de mémoire, Spark en `local[4]`,
`maxOffsetsPerTrigger = 20000`, déclenchement toutes les 5 s. Airflow, Grafana et le tableau de bord tournent en même temps.

## Résultats

| | 10 000 relevés | 100 000 relevés | 1 000 000 de relevés |
|---|---|---|---|
| Stockés / perdus / en double | 10 000 / **0** / **0** | 100 000 / **0** / **0** | 1 000 000 / **0** / **0** |
| Producteur → Kafka | 19 120 év./s | 19 811 év./s | 14 236 év./s |
| Durée de bout en bout (premier message dans Kafka → dernier enregistrement) | 0,9 s | 55,8 s | 786,5 s (13 min) |
| Débit de bout en bout | 11 142 év./s (un seul micro-batch) | **1 792 év./s** | **1 271 év./s** |
| Débit de Spark (moyenne / max par batch, requête `traffic_raw`) | 1 744 / 1 744 év./s | 1 697 / 2 760 év./s | 1 783 / 4 821 év./s |
| Micro-batchs | 1 | 6 | 51 |
| Retard Kafka maximal | 0 | 66 025 messages | 920 007 messages |
| Latence Kafka → PostGIS (p50 / p95 / max) | 0,66 / 0,86 / 0,90 s | 17,8 / 50,8 / 51,0 s | 406 / 696 / 716 s |

**Ressources pendant le benchmark d'1 million** (moyenne / maximum) :

| Conteneur | CPU moyen | CPU max | Mémoire max |
|-----------|-----------|---------|-------------|
| Spark | 117 % | 525 % | 1 691 Mo |
| Kafka | 63 % | 282 % | 1 060 Mo |
| PostGIS | 50 % | 257 % | 686 Mo |
| Airflow | 16 % | 236 % (pendant un passage du DAG) | 1 292 Mo |
| Grafana | 0,5 % | 6 % | 66 Mo |
| Tableau de bord | 0,5 % | 5 % | 43 Mo |

(100 % = un cœur. Les détails par benchmark sont dans les fichiers JSON.)

**En charge normale** (60 capteurs, un relevé toutes les 10 s, environ 6,5 événements/s avec les bus et le tram),
la latence de Kafka à PostGIS est d'environ **__NL_P50__ s (médiane)** et __NL_P95__ s (p95).

**Historique de 24 h au démarrage :** les 505 000 événements simulés au premier démarrage sont traités par Spark
en 8 à 10 minutes, en même temps que les données en direct.

## Lecture des résultats

- **Aucune perte, aucun doublon**, même avec un million d'événements et un retard Kafka de 920 000 messages.
- **10 000 relevés** tiennent dans un seul micro-batch (moins de 20 000 messages) : tout est stocké en moins d'une seconde.
- **Au-delà**, le débit se stabilise autour de **1 300 à 1 800 événements par seconde** de bout en bout.
  Le producteur envoie 10 fois plus vite : les messages attendent dans Kafka, c'est son rôle (tampon durable).
- Pendant un pic, la latence élevée est presque entièrement du **temps d'attente dans Kafka**, pas du temps de traitement :
  chaque micro-batch de 20 000 messages prend entre 4 et 15 secondes.
- Le pipeline a une **marge d'environ 200 fois** par rapport à la charge normale de la ville (6,5 événements/s).

## Goulots d'étranglement

1. **Chaque relevé est lu et analysé deux fois.** La requête brute et la requête des fenêtres lisent le même topic
   indépendamment (deux consommateurs, deux checkpoints). C'est voulu (la table brute garde même les événements au-delà
   du watermark), mais cela double le travail de lecture et de parsing JSON.
2. **Rapatriement sur le driver.** `foreachBatch` fait `collect()` puis des insertions psycopg2 (`execute_values`) :
   tout passe par un seul processus Python. PostGIS monte jusqu'à 2,5 cœurs pendant les insertions.
3. **État de dédoublonnage et des fenêtres.** `dropDuplicates` + fenêtres gardent un état (RocksDB) par `event_id`
   et par (capteur, minute) pendant le watermark : coûteux pendant un pic d'un million d'événements.
4. **Partage des 4 cœurs.** Les quatre requêtes et les 4 threads de Spark se partagent `local[4]`, sur la même
   machine que Kafka, PostGIS et Airflow.
5. **Taille des batchs.** `maxOffsetsPerTrigger = 20000` limite chaque batch, ce qui garde une latence raisonnable
   mais plafonne le débit pendant un rattrapage.

## Pistes d'amélioration

- Une seule lecture du topic, avec un `foreachBatch` qui écrit la table brute et calcule les agrégats (au prix d'une gestion manuelle du watermark).
- Écriture distribuée : chaque partition écrit dans une table de transit (JDBC ou `COPY`), puis un `MERGE` côté PostGIS.
- Plus de partitions Kafka et un vrai cluster Spark (plusieurs exécuteurs).
- Avro au lieu de JSON pour réduire le coût du parsing.
- Augmenter `maxOffsetsPerTrigger` pendant les rattrapages (débit plus élevé, latence par batch plus grande).
