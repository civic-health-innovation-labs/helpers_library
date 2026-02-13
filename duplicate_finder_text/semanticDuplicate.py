from typing import Optional
from sentence_transformers import SentenceTransformer
import numpy as np
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
import logging
from sklearn.cluster import DBSCAN
import faiss

logger = logging.getLogger(__name__)


class SemanticDuplicate:
    def __init__(
            self,
            model_name: str = "all-MiniLM-L6-v2", #default model
            similarity_threshold: float = 0.80, #default similarity threshold
            min_cluster_size: int = 1,
            batch_size: int = 512,
            device: Optional[str] = None,
            data_size_magnitude="small"
    ):
        if not 0 < similarity_threshold <= 1:
            raise ValueError("similarity_threshold must be in (0, 1]")
        if min_cluster_size < 1:
            raise ValueError("min_cluster_size must be >= 1")

        self.model_name = model_name
        self.similarity_threshold = similarity_threshold
        self.min_cluster_size = min_cluster_size
        self.batch_size = batch_size
        self.device = device
        self._model = None
        self.data_size_magnitude = data_size_magnitude

    def fit_transform(
            self,
            df: DataFrame,
            input_col: str,
            cluster_col: str = "semantic_cluster",
    ) -> DataFrame:

        self._validate_input(df, input_col)

        # 1. Extract unique, non-null strings to minimize encoding work
        unique_strings = self._extract_unique_strings(df, input_col)
        n_unique = len(unique_strings)
        logger.info("Unique non-null strings to encode: %d", n_unique)

        if n_unique == 0:
            logger.warning("No non-null strings found; returning with all clusters = -1")
            return df.withColumn(cluster_col, F.lit(-1).cast(IntegerType()))

        # 2. Encode strings → embeddings (driver-side, batched)
        embeddings = self._encode(unique_strings)

        # 3. Cluster embeddings with DBSCAN (cosine distance)
        if self.data_size_magnitude == "large":
            labels = self._cluster_large(embeddings)
        else:
            labels = self._cluster(embeddings)

        # 4. Build a mapping string → cluster_label and broadcast-join
        result_df = self._attach_labels(
            df, input_col, cluster_col, unique_strings, labels
        )
        return result_df

    @staticmethod
    def _validate_input(df: DataFrame, input_col: str) -> None:
        if input_col not in df.columns:
            raise ValueError(f"Column '{input_col}' not found in DataFrame")
        col_type = dict(df.dtypes).get(input_col)
        if col_type != "string":
            raise TypeError(f"Column '{input_col}' must be StringType, got {col_type}")

    @staticmethod
    def _extract_unique_strings(df: DataFrame, input_col: str) -> list[str]:
        """Collect distinct non-null strings from the target column."""
        rows = df.select(input_col).where(F.col(input_col).isNotNull()).distinct().collect()
        return [row[0] for row in rows]

    def _get_model(self):
        """Lazy-load the sentence-transformers model."""
        self._model = SentenceTransformer(
            self.model_name, device=self.device
        )
        return self._model

    def _encode(self, strings: list[str]) -> np.ndarray:
        """Encode a list of strings into normalized embeddings."""
        model = self._get_model()
        logger.info(
            "Encoding %d strings with model '%s' (batch_size=%d)",
            len(strings), self.model_name, self.batch_size,
        )
        embeddings = model.encode(
            strings,
            batch_size=self.batch_size,
            show_progress_bar=True,
            normalize_embeddings=True,  # unit vectors → dot = cosine sim
            convert_to_numpy=True,
        )
        return embeddings

    def _cluster(self, embeddings: np.ndarray) -> np.ndarray:
        """Run DBSCAN with cosine distance on the embeddings. to create clusters of the texts or strings"""
        eps = 1.0 - self.similarity_threshold  # cosine distance threshold
        logger.info("Clustering with DBSCAN (eps=%.4f, min_samples=%d)", eps, self.min_cluster_size,)
        # Precompute cosine distance matrix for efficiency
        cosine_distances = 1 - np.dot(embeddings, embeddings.T)
        # Clamp numerical noise
        np.clip(cosine_distances, 0, 2, out=cosine_distances)

        clustering = DBSCAN(
            eps=eps,
            min_samples=self.min_cluster_size,
            metric="precomputed",
            n_jobs=-1,
        )
        labels = clustering.fit_predict(cosine_distances)

        n_clusters = len(set(labels) - {-1})
        n_noise = int(np.sum(labels == -1))
        logger.info("Found %d clusters and %d noise points", n_clusters, n_noise)
        return labels

    def _cluster_large(self, embeddings: np.ndarray) -> np.ndarray:
        n, dim = embeddings.shape

        # Build FAISS index for fast cosine similarity search
        index = faiss.IndexFlatIP(dim)  # Inner product = cosine (normalized)
        index.add(embeddings.astype(np.float32))

        # Find neighbors within similarity threshold
        # Search for k nearest — adjust k based on expected cluster sizes
        k = min(50, n)
        similarities, indices = index.search(
            embeddings.astype(np.float32), k
        )

        # Build sparse distance graph for DBSCAN
        from scipy.sparse import lil_matrix
        dist_matrix = lil_matrix((n, n), dtype=np.float32)

        for i in range(n):
            for j_idx in range(k):
                j = indices[i][j_idx]
                dist = 1.0 - similarities[i][j_idx]
                dist_matrix[i, j] = max(dist, 0)
                dist_matrix[j, i] = max(dist, 0)

        eps = 1.0 - self.similarity_threshold
        clustering = DBSCAN(
            eps=eps,
            min_samples=self.min_cluster_size,
            metric="precomputed",
            n_jobs=-1,
        )
        return clustering.fit_predict(dist_matrix.tocsr())

    @staticmethod
    def _attach_labels(
            df: DataFrame,
            input_col: str,
            cluster_col: str,
            unique_strings: list[str],
            labels: np.ndarray,
    ) -> DataFrame:
        """
        Create a broadcast lookup table and left-join cluster labels
        back onto the original DataFrame.
        """
        spark = df.sparkSession

        # Build mapping DataFrame
        mapping_data = [
            (s, int(label)) for s, label in zip(unique_strings, labels)
        ]
        mapping_df = spark.createDataFrame(
            mapping_data,
            schema=[f"__{input_col}_key__", cluster_col],
        )

        # Broadcast the (small) mapping table for an efficient join
        result_df = df.join(
            F.broadcast(mapping_df),
            on=df[input_col] == mapping_df[f"__{input_col}_key__"],
            how="left",
        ).drop(f"__{input_col}_key__")

        # Null strings get cluster = -1
        result_df = result_df.withColumn(
            cluster_col,
            F.when(F.col(cluster_col).isNull(), F.lit(-1))
            .otherwise(F.col(cluster_col))
            .cast(IntegerType()),
        )
        return result_df
