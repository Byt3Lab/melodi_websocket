# Image de base Python 3.12 légère
FROM python:3.12-slim

# Configuration des variables d'environnement
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PORT=8070

# Répertoire de travail dans le conteneur
WORKDIR /app

# Installation des dépendances système minimales (curl pour le healthcheck)
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copie et installation des dépendances Python
COPY requirements.txt /app/
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copie du code source du projet
COPY . /app/

# Exposition du port WebSocket par défaut
EXPOSE 8070

# Healthcheck de l'application
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD curl -f http://localhost:${PORT:-8070}/internal/health || exit 1

# Commande de démarrage par défaut avec Hypercorn (serveur ASGI de production)
CMD ["sh", "-c", "exec hypercorn -b 0.0.0.0:${PORT:-8070} -w 1 main:app"]