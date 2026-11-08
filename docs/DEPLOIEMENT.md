# Mettre le tableau de bord en ligne (gratuitement, sur Render)

**Français** · [English](en/DEPLOYMENT.md)

La chaîne complète (Kafka, Spark, PostGIS, Airflow, Grafana) a besoin d'environ 6 Go de mémoire :
c'est trop pour un hébergement gratuit. La version en ligne fonctionne donc ainsi :

```
votre ordinateur : chaîne complète ──export──▶ dashboard/data/snapshot.json ──git push──▶ Render (gratuit)
```

- `scripts/export_snapshot.py` lance **les mêmes requêtes SQL** que le tableau de bord en direct
  (`dashboard/queries.py`), pour chaque période (1 h, 6 h, 24 h), dans un seul instantané cohérent de la base.
- Le résultat est enregistré dans `dashboard/data/snapshot.json` (quelques centaines de Ko), qui est versionné.
- Sur Render, le même conteneur tourne avec `DEMO_MODE=1` : il répond à partir de ce fichier, sans base de données.
  Un bandeau en haut de la page indique la date de la copie.

Aucune base de données en ligne, aucun mot de passe à gérer, aucune carte bancaire. Il faut environ 15 minutes.

---

## Étape 1 : Avoir le projet sur GitHub

Le dépôt doit être sur GitHub (public ou privé). `.env` est dans `.gitignore` : vos mots de passe
locaux ne sont jamais envoyés.

## Étape 2 (facultative) : Rafraîchir la copie des données

Le dépôt contient déjà une copie. Pour en faire une nouvelle à partir de votre propre exécution :

1. Lancez la chaîne et laissez-la tourner (l'historique de 24 h est envoyé automatiquement au premier démarrage).
2. Lancez une fois le DAG Airflow pour que les tables `analytics` soient à jour :

```bash
docker compose exec airflow airflow dags test mobility_analytics
```

3. Exportez la copie (le script lit le mot de passe dans `.env`) :

```bash
python scripts/export_snapshot.py
```

4. Faites un commit de `dashboard/data/snapshot.json` et un push.

## Étape 3 : Créer le service sur Render

1. Allez sur https://render.com et connectez-vous avec votre compte **GitHub**.
2. Cliquez sur **New** → **Blueprint**.
3. Choisissez le dépôt `smart-city-mobility-data-platform`. Render lit le fichier `render.yaml` à la racine :
   - type : service web, plan **Free**
   - image : le `Dockerfile` à la racine du dépôt
   - variable d'environnement : `DEMO_MODE=1`
   - contrôle de santé : `/healthz`
4. Cliquez sur **Apply**. La première construction prend 3 à 5 minutes.
5. Votre tableau de bord est en ligne sur une adresse du type `https://smart-city-mobility-data-platform-1.onrender.com`.

**Sans Blueprint (le plus simple) :** New → Web Service → choisissez le dépôt → Language **Docker** →
gardez les valeurs par défaut (Dockerfile path `./Dockerfile`, Docker build context `.`) → Instance type **Free** →
Create Web Service. Le `Dockerfile` à la racine du dépôt lance déjà le tableau de bord en mode démo.
Ne mettez **pas** `render.yaml` comme Dockerfile path : ce n'est pas un Dockerfile.

## Étape 4 : Ajouter le lien dans le README

En haut de `README.md` et de `README.en.md`, remplacez l'adresse de la démo par la vôtre, puis faites un commit et un push.
Render redéploie automatiquement à chaque push sur `main`.

---

## Bon à savoir

- **Mise en veille :** sur le plan gratuit, Render met le service en veille après 15 minutes sans visite.
  La visite suivante le réveille en 30 à 60 secondes environ. Ouvrez le lien vous-même quelques minutes
  avant de l'envoyer à un recruteur.
- **Autres hébergeurs gratuits :** le même `dashboard/Dockerfile` fonctionne sur d'autres plateformes
  qui acceptent une image Docker (par exemple Koyeb ou Fly.io). Il suffit de définir `DEMO_MODE=1`
  et d'exposer le port donné par la variable `PORT`.
- **Version en direct :** pour un vrai tableau de bord en direct en ligne, il faudrait héberger Kafka,
  Spark et PostGIS (par exemple sur une machine virtuelle). Ce n'est pas possible gratuitement :
  c'est pour cela que la démo en ligne est une copie, et c'est indiqué clairement sur la page.
- **Un problème ?** Dans Render, ouvrez le service → **Logs**.
