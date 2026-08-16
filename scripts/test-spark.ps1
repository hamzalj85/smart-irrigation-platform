# Execute les tests des portes qualite dans l'image du job de streaming :
# meme JVM (Java 17), meme PySpark, meme Python que la production.
#
# On cible le FICHIER et non le marqueur : `-m spark` filtrerait apres la
# collecte, et la collecte importe tous les modules de test -- dont ceux qui
# dependent de numpy ou paho, absents de cette image.
docker compose run --rm --no-deps -v "${PWD}:/work" -w /work `
  --entrypoint sh spark-streaming `
  -c "pip install --quiet pytest && SPARK_LOCAL_IP=127.0.0.1 python -m pytest tests/test_quality_gates.py"