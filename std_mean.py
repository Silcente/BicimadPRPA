import json
from pyspark.sql import SparkSession
import sys
import statistics
from pyspark.sql.types import StructType,IntegerType

spark = SparkSession.builder.getOrCreate()

#Estructura de los datos
schema = StructType()\
    .add("travel_time", IntegerType(), False)\
    .add("ageRange", IntegerType(), False)

#Seleccionar las columnas que necesitamos
def mapper(line):
    data = json.loads(line)
    age = data['ageRange']
    time = data['travel_time']
    return age, time

#La función principal que va a calcular media, desviación típica, máximo y mínimo
def means_stds(data):
    rdd = data.map(mapper)
    result = rdd.groupByKey().mapValues(list)
    means_stds = result.\
        mapValues(lambda x: (statistics.mean(x) if x else 0,  statistics.stdev(x) if len(x) > 1 else 0, max(x) if len(x) > 0 else 0, min(x) if len(x) > 0 else 0))
    return means_stds

#La función que procesa el archivo, teniendo en cuenta la cota máxima elegida para el tiempo
def process_file(file_path, max):
    data = spark.read.json(file_path, schema=schema)
    non_empty_data = data.filter(data["travel_time"].isNotNull() & data["ageRange"].isNotNull())
    filtered = non_empty_data.filter(data["travel_time"] < max)
    rdd = filtered.rdd.map(lambda row: json.dumps(row.asDict()))
    result = means_stds(rdd)
    result_json = result.collectAsMap()
    result_dict = {}
    for age, (mean, std_dev, maximum, minimum) in result_json.items():
        result_dict[age] = {
            "mean": mean,
            "std_dev": std_dev,
            "max":maximum,
            "min":minimum
        }

    output_path = "/user/silcente/results/means_" + file_path.split("/")[-1].split(".")[0] + ".json"
    hadoop = spark._jvm.org.apache.hadoop
    fs = hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
    output_stream = fs.create(hadoop.fs.Path(output_path))
    output_stream.write(json.dumps(result_dict).encode('utf-8'))
    output_stream.close()

#Función principal, encargada de "recoger" cada archivo y llamar a la función process_file
def main(max):
    hadoop = spark._jvm.org.apache.hadoop
    fs = hadoop.fs.FileSystem
    conf = hadoop.conf.Configuration()
    path = hadoop.fs.Path('/public/bicimad')
    files = [str(f.getPath()) for f in fs.get(conf).listStatus(path) if f.getLen() > 0]
    output_dir = "/user/silcente/results"
    fs = hadoop.fs.FileSystem.get(conf)
    if not fs.exists(hadoop.fs.Path(output_dir)):
        fs.mkdirs(hadoop.fs.Path(output_dir))
    for file in files:
        process_file(file, max)
    spark.stop()

#Recoge el valor máximo para el tiempo y llama al main
if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Quantity needed")
    else:
        print(sys.argv[1])
        main(int(sys.argv[1]))