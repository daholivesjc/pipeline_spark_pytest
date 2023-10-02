from src.pipeline import PipelineSpark

registros = [
    (1, "Alice", 25),
    (2, "Bob", 30),
    (3, "Charlie", 35)
]

schema = ["ID", "Nome", "Idade"]

pipeline = PipelineSpark(
    data = registros,
    schema = schema
)

pipeline.run()