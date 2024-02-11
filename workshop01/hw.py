import dlt
import duckdb

# Q1
def square_root_generator(limit):
    n = 1
    while n <= limit:
        yield n ** 0.5
        n += 1

limit = 5
generator = square_root_generator(limit)
print(sum(generator))

# Q2
for sqrt_value in square_root_generator(13):
    print(sqrt_value)

# Q3
def people_1():
    for i in range(1, 6):
        yield {"ID": i, "Name": f"Person_{i}", "Age": 25 + i, "City": "City_A"}

def people_2():
    for i in range(3, 9):
        yield {"ID": i, "Name": f"Person_{i}", "Age": 30 + i, "City": "City_B", "Occupation": f"Job_{i}"}

pipeline = dlt.pipeline(pipeline_name="people_import", destination='duckdb', dataset_name='people_info')
conn = duckdb.connect(f"{pipeline.pipeline_name}.duckdb")

pipeline.run(people_1(), table_name="ppl", write_disposition="replace")
pipeline.run(people_2(), table_name="ppl", write_disposition="append")

print(conn.sql("SELECT sum(age) FROM ppl").df())

# Q4
pipeline.run(people_1(), table_name="ppl", write_disposition="replace", primary_key='id')
pipeline.run(people_2(), table_name="ppl", write_disposition="merge", primary_key='id')
print(conn.sql("SELECT sum(age) FROM ppl").df())
