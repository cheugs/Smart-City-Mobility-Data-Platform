# Checklist qualité et acceptation (DE02)

**Français** · [English](en/CHECKLIST.md)

La checklist d'origine parle de *transactions financières*. Ce projet traite des **événements de mobilité**
(relevés de capteurs de trafic et passages de bus et de tram) : chaque critère est appliqué à ces événements,
et la section 8 (« analyse financière ») est adaptée à l'analyse de la mobilité. Chaque ligne renvoie à la
preuve : le code, le test automatisé ou la mesure.

<img src="assets/icons/check.svg" width="14"> = vérifié par un test automatisé ou une mesure documentée ·
<img src="assets/icons/partial.svg" width="14"> = traité, avec une limite expliquée

Abréviations : **U** = test unitaire (`tests/unit`), **I** = test d'intégration (`tests/integration`),
**E2E** = test de bout en bout sur la chaîne Docker (`tests/e2e`), **AF** = contrôle qualité Airflow
([data_quality_checks.sql](../airflow/sql/checks/data_quality_checks.sql)).

## 1. Exactitude fonctionnelle

| | Critère | Preuve |
|---|---|---|
| <img src="assets/icons/check.svg" width="14"> | Les événements sont générés avec des attributs réalistes | Heures de pointe, relation vitesse/densité, incidents, retards des bus ([generator.py](../producer/generator.py)) · U `test_rush_hour_is_slower_and_busier_than_the_night`, `test_transit_vehicles_follow_the_timetable_with_a_delay` |
| <img src="assets/icons/check.svg" width="14"> | Chaque événement a un identifiant unique | UUID v4 + clé primaire `event_id` · U `test_readings_have_unique_ids_and_valid_values` · AF `no_duplicate_*_event_ids` |
| <img src="assets/icons/check.svg" width="14"> | Les mesures, unités et horodatages sont valides | Règles de validation + contraintes `CHECK` · U `test_each_traffic_rule`, `test_each_transit_rule` · AF `measurements_in_valid_range` |
| <img src="assets/icons/check.svg" width="14"> | Les événements sont ingérés par Kafka | 2 topics créés par `kafka-init` · E2E `test_valid_readings_are_ingested_with_their_kafka_position` |
| <img src="assets/icons/check.svg" width="14"> | Spark traite les événements en continu | 4 requêtes Structured Streaming, déclenchement toutes les 5 s ([streaming_job.py](../spark/streaming_job.py)) · E2E `test_monitoring_tables_show_progress_lag_and_starts` |
| <img src="assets/icons/check.svg" width="14"> | Les agrégations sont mathématiquement justes | U `test_counts_sums_and_averages_per_sensor_and_minute`, `test_transit_windows_delay_and_on_time` |
| <img src="assets/icons/check.svg" width="14"> | PostgreSQL stocke les résultats attendus | E2E `test_duplicates_are_stored_once_and_do_not_inflate_windows`, `test_transit_delay_is_computed_and_stored` |
| <img src="assets/icons/check.svg" width="14"> | Les valeurs du tableau de bord correspondent à la base | E2E `test_dashboard_figures_match_the_database` (même instantané de la base + API HTTP) |

## 2. Qualité et intégrité des données

| | Critère | Preuve |
|---|---|---|
| <img src="assets/icons/check.svg" width="14"> | Les champs obligatoires manquants sont détectés | `missing_event_id`, `missing_sensor_id`, `missing_event_time`, `missing_measurement` · U `test_each_traffic_rule` |
| <img src="assets/icons/check.svg" width="14"> | Les valeurs invalides sont rejetées (équivalent des montants invalides) | Véhicules négatifs, vitesse hors limites, occupation > 100 %, voyageurs hors limites · U + E2E `test_invalid_records_are_isolated_with_their_reason` |
| <img src="assets/icons/check.svg" width="14"> | Les horodatages invalides sont gérés | `invalid_event_time`, `event_time_in_future`, `invalid_scheduled_time` · U + E2E |
| <img src="assets/icons/check.svg" width="14"> | Les identifiants en double sont détectés | `ON CONFLICT ... RETURNING` compte les doublons dans `monitoring.batch_log.duplicates_skipped` · E2E |
| <img src="assets/icons/check.svg" width="14"> | Les doublons ne gonflent pas les agrégats | `dropDuplicates` avec état · U `test_duplicate_event_ids_are_counted_once` · I `test_duplicates_across_batches_are_counted_once` · AF `windows_not_inflated_by_duplicates` |
| <img src="assets/icons/check.svg" width="14"> | Les enregistrements invalides sont isolés des valides | Table `raw.bad_events` avec motif et position Kafka · I `test_malformed_and_invalid_records_are_isolated_and_stream_keeps_running` |
| <img src="assets/icons/check.svg" width="14"> | Les transformations préservent l'intégrité | Clés étrangères vers `city.*`, contraintes, AF `transit_delay_matches_timestamps`, `window_vehicle_totals_not_inflated` |
| <img src="assets/icons/check.svg" width="14"> | Les règles de qualité sont testées automatiquement | 30+ cas unitaires de validation + 10 contrôles Airflow toutes les 10 min (résultats dans `monitoring.dq_results`) |

## 3. Traitement en temps réel

| | Critère | Preuve |
|---|---|---|
| <img src="assets/icons/check.svg" width="14"> | Les événements sont traités sans exécution batch | Micro-batchs Structured Streaming toutes les 5 s ; latence médiane mesurée (section 9 du README) |
| <img src="assets/icons/check.svg" width="14"> | Le temps d'événement est distingué du temps de traitement | 3 horodatages stockés (`event_time`, `kafka_time`, `processed_at`) ; fenêtres sur `event_time` |
| <img src="assets/icons/check.svg" width="14"> | Les événements dans le désordre sont bien gérés | I `test_out_of_order_events_land_in_the_right_window` |
| <img src="assets/icons/check.svg" width="14"> | Les événements en retard sont gérés avec un watermark | I `test_late_event_within_watermark_updates_its_old_window`, `test_event_beyond_watermark_is_dropped_from_windows_but_kept_raw` · E2E `test_late_event_within_the_watermark_updates_its_window` |
| <img src="assets/icons/check.svg" width="14"> | Les agrégations par fenêtre sont exactes | U `test_aggregation.py` · E2E (fenêtre = nombre de mesures distinctes) |
| <img src="assets/icons/check.svg" width="14"> | La latence est mesurable | `processed_at - kafka_time` : p50/p95 dans Grafana, le tableau de bord et les benchmarks |
| <img src="assets/icons/check.svg" width="14"> | Le débit est mesurable | `monitoring.stream_progress.processed_rows_per_second` + benchmarks |
| <img src="assets/icons/check.svg" width="14"> | Le retard du consommateur Kafka est observable | `kafka_lag` calculé à chaque batch (offset le plus récent - offset lu) ; panneau Grafana |

## 4. Fiabilité et tolérance aux pannes

| | Critère | Preuve |
|---|---|---|
| <img src="assets/icons/check.svg" width="14"> | Le pipeline reprend après un redémarrage | E2E `test_spark_restart_loses_nothing_and_counts_nothing_twice` |
| <img src="assets/icons/check.svg" width="14"> | Les checkpoints conservent la progression | I `test_restart_from_checkpoint_keeps_state` ; événement `restart_from_checkpoint` dans `monitoring.pipeline_events` |
| <img src="assets/icons/check.svg" width="14"> | Les traitements en échec peuvent être relancés | `with_retry` (3 tentatives) dans Spark ; `retries: 2` sur chaque tâche Airflow ; `restart: unless-stopped` · E2E lent `test_postgres_outage_is_survived` |
| <img src="assets/icons/check.svg" width="14"> | Un double traitement ne corrompt pas la base | Upserts idempotents ; `batch_log` dans la même transaction · I + E2E de redémarrage |
| <img src="assets/icons/check.svg" width="14"> | Les événements historiques peuvent être rejoués | Rétention Kafka de 7 jours + [scripts/replay.ps1](../scripts/replay.ps1) · E2E lent `test_full_replay_from_kafka_gives_back_the_same_events` |
| <img src="assets/icons/check.svg" width="14"> | La reprise ne perd aucun événement en silence | `failOnDataLoss=true` ; comptages exacts dans les tests de redémarrage et les benchmarks (0 perdu) |
| <img src="assets/icons/check.svg" width="14"> | Un changement de schéma ne casse pas le traitement | v1, v2, sans version et v3 inconnue dans le même flux · U, I `test_schema_evolution_mixed_versions_in_one_stream`, E2E · AF `v1_readings_have_no_v2_fields` |
| <img src="assets/icons/check.svg" width="14"> | Les garanties de livraison sont documentées et testées | Tableau de la section 6 du README ; tests de doublons, de redémarrage et de rejeu |

## 5. Architecture des données

| | Critère | Preuve |
|---|---|---|
| <img src="assets/icons/check.svg" width="14"> | Chaque composant a une responsabilité claire | Tableau des composants (section 4 du README) |
| <img src="assets/icons/check.svg" width="14"> | Les topics et partitions Kafka sont bien configurés | 2 topics, 3 partitions, clé `sensor_id` / `line_id` (ordre par capteur), rétention 7 jours, création automatique désactivée |
| <img src="assets/icons/check.svg" width="14"> | Les flux de données sont traçables | `kafka_partition` + `kafka_offset` sur chaque ligne brute et chaque rejet ; `batch_log`, `stream_progress` (`run_id`), `pipeline_events` |
| <img src="assets/icons/check.svg" width="14"> | Les données brutes et transformées sont séparées | Schémas `city`, `raw`, `stream`, `monitoring`, `analytics` ([01_schema.sql](../sql/01_schema.sql)) |
| <img src="assets/icons/check.svg" width="14"> | Les tables PostgreSQL sont bien structurées | Clés primaires, clés étrangères, contraintes `CHECK`, types géométriques avec SRID |
| <img src="assets/icons/check.svg" width="14"> | Les index sont justifiés | Commentaires dans `01_schema.sql` ; index GiST pour les jointures spatiales ; index partiel unique pour les incidents |
| <img src="assets/icons/partial.svg" width="14"> | Les transformations (dbt dans la checklist) sont modulaires | dbt ne fait pas partie de la pile demandée (Kafka, Python, Spark, PostGIS, Airflow, Grafana). Équivalent : un fichier SQL par mart, une tâche Airflow par fichier, des dépendances explicites ([mobility_analytics.py](../airflow/dags/mobility_analytics.py)) · U `test_every_mart_file_is_used_by_the_dag` |
| <img src="assets/icons/check.svg" width="14"> | L'architecture permet des extensions | Nouveau topic = nouvelle requête ; nouveau mart = un fichier SQL ; nouvelle ville = `generate_city.py` ; champs de schéma optionnels |

## 6. Performance et passage à l'échelle

| | Critère | Preuve |
|---|---|---|
| <img src="assets/icons/check.svg" width="14"> | Le pipeline traite au moins 10 000 événements | [benchmarks/benchmark_10000.json](../benchmarks/benchmark_10000.json) |
| <img src="assets/icons/check.svg" width="14"> | Le pipeline est testé avec 100 000 événements | [benchmarks/benchmark_100000.json](../benchmarks/benchmark_100000.json) |
| <img src="assets/icons/check.svg" width="14"> | Un benchmark d'1 million d'événements est tenté | [benchmarks/benchmark_1000000.json](../benchmarks/benchmark_1000000.json) |
| <img src="assets/icons/check.svg" width="14"> | Le débit est mesuré en événements par seconde | Producteur, Spark (moyenne par batch) et bout en bout ([PERFORMANCE.md](PERFORMANCE.md)) |
| <img src="assets/icons/check.svg" width="14"> | La latence de traitement est mesurée | p50, p95 et max par benchmark et en charge normale |
| <img src="assets/icons/check.svg" width="14"> | La consommation de ressources est surveillée | `docker stats` toutes les 5 s pendant les benchmarks (CPU, mémoire par conteneur) |
| <img src="assets/icons/check.svg" width="14"> | Les goulots d'étranglement sont identifiés | [PERFORMANCE.md](PERFORMANCE.md), section « Goulots d'étranglement » |
| <img src="assets/icons/check.svg" width="14"> | Les résultats sont documentés | Section 9 du README + [PERFORMANCE.md](PERFORMANCE.md) |

## 7. Supervision et observabilité

| | Critère | Preuve |
|---|---|---|
| <img src="assets/icons/check.svg" width="14"> | L'ingestion est surveillée | Grafana « Events per second », « Throughput per streaming query », « Readings received vs expected » |
| <img src="assets/icons/check.svg" width="14"> | Les échecs de traitement sont journalisés | Logs Python de chaque conteneur ; `query_failed` dans `monitoring.pipeline_events` ; contrôles `FAIL` dans `monitoring.dq_results` |
| <img src="assets/icons/check.svg" width="14"> | Le retard Kafka est surveillé | Panneaux Grafana + page Pipeline du tableau de bord + badge de santé |
| <img src="assets/icons/check.svg" width="14"> | Les délais de traitement sont surveillés | Latence p50/p95 par minute (Grafana) ; événements en retard par minute |
| <img src="assets/icons/check.svg" width="14"> | Les doublons sont comptés | `monitoring.batch_log.duplicates_skipped` (Grafana + page Qualité) |
| <img src="assets/icons/check.svg" width="14"> | Les événements invalides sont comptés | `raw.bad_events` par motif (Grafana + page Qualité) |
| <img src="assets/icons/check.svg" width="14"> | Les reprises sont traçables | `fresh_start`, `restart_from_checkpoint`, `replay_requested` + nouveau `run_id` |
| <img src="assets/icons/check.svg" width="14"> | Les logs suffisent pour enquêter | Chaque batch logue reçus / nouveaux / doublons / invalides ; chaque rejet garde son message brut et sa position Kafka |

## 8. Analyse de la mobilité (adaptation de « analyse financière »)

| | Critère d'origine → critère adapté | Preuve |
|---|---|---|
| <img src="assets/icons/check.svg" width="14"> | Volumes de transactions → volumes de véhicules et de passages | KPI « Véhicules comptés », graphiques par type de voie, passages par ligne |
| <img src="assets/icons/check.svg" width="14"> | Montants totaux → totaux par tronçon, quartier et ligne | `analytics.traffic_segment_15min`, `traffic_district_hourly`, `transit_line_hourly` |
| <img src="assets/icons/check.svg" width="14"> | Répartition par statut → répartition par niveau de congestion, statut de capteur, ponctualité | Niveaux fluide / dense / congestionné ; OK / DELAYED / SILENT ; à l'heure / en retard |
| <img src="assets/icons/check.svg" width="14"> | Volumes par fenêtre de temps | `stream.traffic_per_minute`, `stream.transit_per_5min` ; profil horaire |
| <img src="assets/icons/check.svg" width="14"> | Différences de devises → différences entre types de voies | **Vitesse relative** (vitesse / limite) pour comparer une rue à 30 km/h et un boulevard à 70 km/h ; jamais de moyenne brute entre classes |
| <img src="assets/icons/check.svg" width="14"> | Indicateurs simulés d'activité suspecte → indicateurs simulés d'anomalies | Incidents simulés (route ralentie), capteurs silencieux, valeurs impossibles, tronçons congestionnés |
| <img src="assets/icons/check.svg" width="14"> | Les indicateurs ont des règles explicables | Seuils documentés (section 7 du README) et visibles dans le SQL |
| <img src="assets/icons/check.svg" width="14"> | Aucune prétention de détection réelle | Avertissement en haut du README et sur le tableau de bord : données synthétiques, aucune mesure ni prévision du trafic réel |

## 9. Tests et validation

| | Critère | Preuve |
|---|---|---|
| <img src="assets/icons/check.svg" width="14"> | Les tests unitaires passent | `docker compose run --rm tests` + CI |
| <img src="assets/icons/check.svg" width="14"> | Les tests d'intégration passent | `tests/integration` (vraies requêtes de streaming) + CI |
| <img src="assets/icons/check.svg" width="14"> | Les tests de bout en bout passent | 11 tests `pytest tests/e2e -m "not slow"` + CI (chaîne Docker complète) ; 2 tests longs `-m slow` passés en local |
| <img src="assets/icons/check.svg" width="14"> | Scénarios de doublons testés | U, I, E2E |
| <img src="assets/icons/check.svg" width="14"> | Scénarios d'événements en retard testés | I (dans et au-delà du watermark), E2E |
| <img src="assets/icons/check.svg" width="14"> | Scénarios d'événements dans le désordre testés | I |
| <img src="assets/icons/check.svg" width="14"> | Scénarios de panne testés | E2E redémarrage de Spark ; E2E lent panne de Postgres |
| <img src="assets/icons/check.svg" width="14"> | Scénarios de rejeu testés | E2E lent `test_full_replay_from_kafka_gives_back_the_same_events` |
| <img src="assets/icons/check.svg" width="14"> | Scénarios d'évolution du schéma testés | U, I, E2E |
| <img src="assets/icons/check.svg" width="14"> | Les chiffres du tableau de bord sont validés par rapport à la base | E2E `test_dashboard_figures_match_the_database` |

## 10. Qualité d'ingénierie et professionnalisme

| | Critère | Preuve |
|---|---|---|
| <img src="assets/icons/check.svg" width="14"> | Le code est modulaire et maintenable | Logique Spark séparée des entrées/sorties ; un fichier SQL par mart ; requêtes du tableau de bord partagées entre direct et démo |
| <img src="assets/icons/check.svg" width="14"> | La configuration est externalisée | `.env` + variables d'environnement ([.env.example](../.env.example)) |
| <img src="assets/icons/check.svg" width="14"> | Aucun secret n'est sur GitHub | `.env` dans `.gitignore` ; CI avec des mots de passe aléatoires ; démo en ligne sans base ni mot de passe |
| <img src="assets/icons/check.svg" width="14"> | Les services Docker sont reproductibles | Versions d'images et de paquets figées ; une seule commande |
| <img src="assets/icons/check.svg" width="14"> | La CI lance automatiquement les tests | [.github/workflows/ci.yml](../.github/workflows/ci.yml) |
| <img src="assets/icons/check.svg" width="14"> | Le README explique l'architecture | Section 4 (schéma, tableau, flux pas à pas) |
| <img src="assets/icons/check.svg" width="14"> | Les choix et compromis sont documentés | Section 8 du README |
| <img src="assets/icons/check.svg" width="14"> | Les limites connues sont reconnues | Section 14 du README |
| <img src="assets/icons/check.svg" width="14"> | Un autre développeur peut reproduire le projet | Section 13 du README ; la CI reconstruit tout à partir de zéro à chaque push |
