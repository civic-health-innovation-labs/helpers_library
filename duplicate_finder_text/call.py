from pyspark.sql import DataFrame
from semanticDuplicate import SemanticDuplicate

def find_semantic_duplicates(
    df: DataFrame,
    input_col: str,
    cluster_col: str = "semantic_cluster",
    model_name: str = "all-MiniLM-L6-v2",
    similarity_threshold: float = 0.80,
    min_cluster_size: int = 2,
    batch_size: int = 512,
) -> DataFrame:
    clusterer = SemanticDuplicate(
        model_name=model_name,
        similarity_threshold=similarity_threshold,
        min_cluster_size=min_cluster_size,
        batch_size=batch_size,
    )
    return clusterer.fit_transform(df, input_col, cluster_col)
