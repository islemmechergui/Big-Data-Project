from pyspark import SparkContext

# Initialisation du SparkContext
sc = SparkContext("local", "Analyse Consommation Eau")

print("\n==> Lecture du fichier CSV...")
rdd = sc.textFile("cleaned_global_water_consumption.csv")

# Suppression de l'en-tête
header = rdd.first()
rdd_cleaned = rdd.filter(lambda line: line != header)
print("\n==> En-tête supprimée. Nombre de lignes restantes :", rdd_cleaned.count())

# Fonction de parsing
def parse_line(line):
    parts = line.split(",")
    try:
        return (
            parts[0],                  # Country
            int(parts[1]),             # Year
            float(parts[2]),           # Total consumption
            float(parts[3]),           # Agricultural
            float(parts[4]),           # Industrial
            float(parts[5]),           # Domestic
            float(parts[6]),           # Rainfall
            float(parts[7]),           # Groundwater depletion
            parts[8]                   # Country Type
        )
    except:
        return None

print("\n==> Transformation des lignes (split + conversion des types)...")
rdd_parsed = rdd_cleaned.map(parse_line).filter(lambda x: x is not None)
print("Données valides après parsing :", rdd_parsed.count())

# Extraire les paires (pays, consommation totale)
print("\n==> Mapping des données pour extraire (pays, consommation)...")
rdd_mapped = rdd_parsed.map(lambda x: (x[0], x[2]))
print("Exemple :", rdd_mapped.take(3))

# Filtrage des valeurs nulles ou aberrantes
print("\n==> Filtrage des consommations nulles ou négatives...")
rdd_filtered = rdd_mapped.filter(lambda x: x[1] > 0)
print("Données après filtrage :", rdd_filtered.count())

# Réduction pour calculer la consommation totale par pays
print("\n==> Réduction des données (consommation totale par pays)...")
rdd_reduced = rdd_filtered.reduceByKey(lambda a, b: a + b)
print("Exemple de résultats réduits :", rdd_reduced.take(5))

# Tri alphabétique par pays
print("\n==> Tri alphabétique des pays...")
rdd_sorted = rdd_reduced.sortByKey()
print("\n Top 10 des pays triés alphabétiquement :")
for country in rdd_sorted.take(10):
    print(country)

# Fin du traitement
print("\n==> Partie 1 terminée : Manipulation des RDD effectuée avec succès.")
