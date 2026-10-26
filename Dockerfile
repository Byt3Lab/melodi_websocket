# Image de base Python 3.12 légère
FROM python:3.12-slim

# Configuration des variables d'environnement
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PORT=8070

# Répertoire de travail dans le conteneur
WORKDIR /app

# Installation des dépendances système requises par psycopg2, reportlab et outils associés
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    libpq-dev \
    zbar-tools \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copie et installation des dépendances Python
COPY requirements.txt /app/
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copie du code source du projet
COPY . /app/

# Exposition du port Web de MELODI (8000)
EXPOSE 8070

# Healthcheck de l'application
HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
  CMD curl -f http://localhost:8070/ || exit 1

# Commande de démarrage par défaut avec Hypercorn (serveur ASGI de production)
CMD ["hypercorn", "-b", "0.0.0.0:8070", "-w", "1", "main:app"]