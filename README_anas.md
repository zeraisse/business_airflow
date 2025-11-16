# Projet Business Airflow - Guide de Lancement

Ce document explique comment configurer et lancer l'environnement Airflow pour le projet de scraping modulaire.

## 1. Prérequis

Assurez-vous d'avoir les outils suivants installés sur votre machine :
-   [Docker](https://www.docker.com/get-started)
-   [Docker Compose](https://docs.docker.com/compose/install/)

## 2. Configuration Initiale

Avant de lancer les conteneurs, vous devez créer un fichier `.env` à la racine du projet pour définir l'identifiant de l'utilisateur qui exécutera Airflow. Cela évite les problèmes de permissions sur les fichiers générés (logs, HTML, etc.).

1.  À la racine du projet (`business_airflow/`), créez un fichier nommé `.env`.
2.  Ajoutez la ligne suivante à ce fichier :

    ```
    AIRFLOW_UID=50000
    ```

    > **Note** : Sur Linux ou macOS, vous pouvez utiliser votre propre identifiant utilisateur en exécutant la commande `echo -e "AIRFLOW_UID=$(id -u)" > .env` dans le terminal à la racine du projet.

## 3. Lancement des Services

Une fois le fichier `.env` créé, vous pouvez démarrer tous les services Airflow (scheduler, webserver, workers...) avec une seule commande.

1.  Ouvrez un terminal à la racine du projet (`business_airflow/`).
2.  Exécutez la commande suivante :

    ```bash
    docker-compose up --build -d
    ```

    -   `--build` : Reconstruit l'image Docker d'Airflow pour inclure vos derniers changements de code.
    -   `-d` : Lance les conteneurs en arrière-plan.

Le premier démarrage peut prendre quelques minutes.

## 4. Accès et Configuration de l'Interface Airflow

Une fois les conteneurs démarrés, vous pouvez accéder à l'interface web d'Airflow.

-   **URL** : http://localhost:8082
-   **Identifiant** : `airflow`
-   **Mot de passe** : `airflow`

Avant de lancer les workflows, vous devez configurer deux "Pools" pour gérer la simultanéité des tâches.

#### a) Créer le Pool pour les Proxies

Ce pool limite le nombre de tests de proxies simultanés à 20, comme demandé dans les contraintes.

1.  Dans l'interface Airflow, allez dans `Admin` -> `Pools`.
2.  Cliquez sur le bouton `+` pour ajouter un nouveau pool.
3.  Remplissez les champs :
    -   **Pool Name** : `proxy_check_pool` (le nom doit être exact)
    -   **Slots** : `20`
4.  Cliquez sur `Save`.

#### b) Créer le Pool pour le Scraping

Ce pool limite le nombre de téléchargements de pages web en parallèle pour ne pas surcharger le site cible.

1.  Retournez dans `Admin` -> `Pools`.
2.  Cliquez sur `+`.
3.  Remplissez les champs :
    -   **Pool Name** : `scraping_pool`
    -   **Slots** : `10` (ou une autre valeur raisonnable)
4.  Cliquez sur `Save`.

## 5. Exécution des Workflows (DAGs)

Les DAGs doivent être lancés dans un ordre précis car le scraping dépend des proxies.

#### Étape 1 : Lancer le `proxy_manager_dag`

Ce DAG est le **service de fourniture de proxies**. Il va scraper les sites de proxies, les tester et stocker les proxies valides pour que les autres services puissent les utiliser.

1.  Sur la page d'accueil d'Airflow, trouvez le DAG `proxy_manager_dag`.
2.  Activez-le en cliquant sur le bouton à gauche (il passera de "paused" à "running").
3.  Pour un lancement immédiat, cliquez sur le bouton "Play" (▶️) à droite et choisissez `Trigger DAG`.

Attendez que ce DAG se termine avec succès (il passera au vert). Vous pouvez vérifier qu'il a bien fonctionné en allant dans `Admin` -> `Variables` : une variable nommée `proxy_list` doit avoir été créée.

#### Étape 2 : Lancer le `scrape_daily_kbo_dynamic`

Ce DAG est le **service de scraping**. Il utilise les proxies fournis par le `proxy_manager` pour télécharger les pages des entreprises.

1.  Sur la page d'accueil, trouvez le DAG `scrape_daily_kbo_dynamic`.
2.  Activez-le en cliquant sur le bouton de pause.
3.  Comme il est programmé pour s'exécuter toutes les minutes, il se lancera automatiquement. Vous pouvez aussi le déclencher manuellement avec le bouton "Play" (▶️).

## 6. Vérification des Résultats

-   **Logs des tâches** : Cliquez sur un run du DAG `scrape_daily_kbo_dynamic`, puis sur une des tâches `scrape_one_enterprise` et enfin sur l'onglet "Log" pour voir les détails de l'exécution.
-   **Fichiers HTML** : Les fichiers HTML téléchargés apparaîtront dans le dossier `data/html/` de votre projet sur votre machine.

## 7. Arrêt des Services

Pour arrêter tous les conteneurs Airflow, exécutez la commande suivante à la racine du projet :

```bash
docker-compose down
```