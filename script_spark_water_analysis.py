# Partie 1 : Manipulation des RDD avec PySpark
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, stddev, desc

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




# Initialisation de SparkSession
spark = SparkSession.builder \
    .appName("Analyse Consommation Eau - Partie 2") \
    .getOrCreate()

print("\n==> Conversion des RDD en DataFrame Spark...")

# Convertir l'ensemble de données en DataFrame Spark
columns = ["Country", "Year", "Total_Consumption", "Agricultural", "Industrial", 
           "Domestic", "Rainfall", "Groundwater_Depletion", "Country_Type"]
df = spark.createDataFrame(rdd_parsed, columns)

# Créer une vue temporaire pour les requêtes SQL
df.createOrReplaceTempView("water_consumption")
print("Vue temporaire 'water_consumption' créée avec succès.")

# 1. Identifier les pays avec la consommation d’eau la plus stable au fil des années
print("\n==> Requête 1 : Pays avec la consommation d'eau la plus stable...")
query_stable_consumption = """
SELECT Country, AVG(Total_Consumption) AS Avg_Consumption, STDDEV(Total_Consumption) AS Stability
FROM water_consumption
GROUP BY Country
ORDER BY Stability ASC
LIMIT 10
"""
stable_consumption_df = spark.sql(query_stable_consumption)
print("Pays avec la consommation d'eau la plus stable :")
stable_consumption_df.show()

# 2. Étudier les tendances de consommation d’eau dans les régions arides
print("\n==> Requête 2 : Tendances de consommation dans les régions arides...")
query_arid_regions = """
SELECT Country, AVG(Total_Consumption) AS Avg_Consumption, AVG(Rainfall) AS Avg_Rainfall
FROM water_consumption
WHERE Rainfall < 500  -- Seuil pour identifier les régions arides (en mm/an)
GROUP BY Country
ORDER BY Avg_Consumption DESC
LIMIT 10
"""
arid_regions_df = spark.sql(query_arid_regions)
print("Tendances de consommation dans les régions arides :")
arid_regions_df.show()

# 3. Analyser les pics de consommation d’eau et proposer des explications
print("\n==> Requête 3 : Analyse des pics de consommation d'eau...")
query_peaks = """
SELECT Country, Year, Total_Consumption
FROM water_consumption
WHERE Total_Consumption > (SELECT AVG(Total_Consumption) * 2 FROM water_consumption)
ORDER BY Total_Consumption DESC
LIMIT 10
"""
peaks_df = spark.sql(query_peaks)
print("Pics de consommation d'eau :")
peaks_df.show()

# 4. Comparer la consommation d’eau entre pays développés et en développement
print("\n==> Requête 4 : Comparaison entre pays développés et en développement...")
query_developed_vs_developing = """
SELECT Country_Type, AVG(Total_Consumption) AS Avg_Consumption
FROM water_consumption
GROUP BY Country_Type
"""
developed_vs_developing_df = spark.sql(query_developed_vs_developing)
print("Comparaison de la consommation d'eau entre pays développés et en développement :")
developed_vs_developing_df.show()

# 5. Déterminer si les politiques de conservation de l’eau ont eu un impact significatif
print("\n==> Requête 5 : Impact des politiques de conservation de l'eau...")
query_conservation_impact = """
SELECT Country, AVG(Total_Consumption) AS Avg_Consumption, 
       AVG(Groundwater_Depletion) AS Avg_Groundwater_Depletion
FROM water_consumption
GROUP BY Country
HAVING AVG(Groundwater_Depletion) < 100  -- Seuil pour identifier les pays avec politiques efficaces
ORDER BY Avg_Consumption ASC
LIMIT 10
"""
conservation_impact_df = spark.sql(query_conservation_impact)
print("Impact des politiques de conservation de l'eau :")
conservation_impact_df.show()

# Fin du traitement
print("\n==> Partie 2 terminée : Analyse des données avec DataFrame et SQL effectuée avec succès.")
