# Projet Spark & HDFS avec Docker

## Prérequis

Avant de commencer, assurez-vous d’avoir :  
1. Un IDE (ex : VS Code, PyCharm)  
2. Un terminal  
3. Docker et Docker Compose installés sur votre machine  

---

## Étape 1 : Cloner le projet

1. Ouvrez un terminal.  
2. Clonez le projet :  

```bash
git clone <URL_DU_PROJET>
```

3. Allez dans le dossier du projet :

```bash
cd <NOM_DU_PROJET>
```

## Étape 2 : Lancer les conteneurs Docker

1. Depuis la racine du projet, lancez la commande suivante pour démarrer tous les services en arrière-plan :

```bash
docker compose up -d
```

En cas de problème ou si vous devez réinitialiser les volumes Docker, utilisez la commande suivante :

```bash
docker compose down -v
```

⚠️ Cette commande supprime les volumes associés, donc toutes les données non sauvegardées dans HDFS seront perdues.

## Étape 3 : Préparer le HDFS

1. Ouvrez un terminal et entrez dans le conteneur **namenode** :

```bash
docker compose exec namenode bash
```

2. Créez le répertoire nécessaire dans HDFS :

```bash
hdfs dfs -mkdir -p /mr/donne
```

3. Uploadez les fichiers dans HDFS :

```bash
hdfs dfs -put -f /Fichier_partager/donne/u.item /mr/donne/
hdfs dfs -put -f /Fichier_partager/donne/u.data /mr/donne/
hdfs dfs -put -f /Fichier_partager/donne/u.genre /mr/donne/
```

⚠️ Assurez-vous que les fichiers u.user et u.data existent bien dans le dossier /Fichier_partager/donne/ de votre projet avant de les uploader.

## Étape 4 : Lancer l’analyse Spark

1. Ouvrez un nouveau terminal et entrez dans le conteneur **spark-master** :

```bash
docker compose exec spark-master bash
```

2. Lancez le script Spark pour analyser les notes de films :

```bash
 /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    /Fichier_partager/analyse_NoteFilm_spark.py
```

⚠️ Assurez-vous que tous les conteneurs sont bien démarrés avant d’exécuter cette commande et que le fichier analyse_NoteFilm_spark.py existe dans /Fichier_partager/.

## Étape 5 : Notes importantes

- Assurez-vous que tous les conteneurs Docker sont bien démarrés avant d’exécuter le script Spark.  
- Vérifiez que les chemins suivants existent dans votre projet :  
  - `/Fichier_partager/donne/` (pour les fichiers `u.user` et `u.data`)  
  - `/Fichier_partager/analyse_NoteFilm_spark.py` (le script Spark)  
- En cas de problème avec Docker ou HDFS, vous pouvez réinitialiser les conteneurs et volumes avec :  

```bash
docker compose down -v
docker compose up -d
```

⚠️ L’utilisation de docker compose down -v supprime les volumes, donc toutes les données non sauvegardées dans HDFS seront perdues.
