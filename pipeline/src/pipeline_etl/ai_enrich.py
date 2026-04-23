from __future__ import annotations

import os
import re

import pandas as pd
from sklearn.cluster import KMeans
from sklearn.feature_extraction.text import TfidfVectorizer


def _normalize_token(token: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", token.lower())


def _cluster_label(terms: list[str]) -> str:
    cleaned = [_normalize_token(term) for term in terms if term]
    cleaned = [term for term in cleaned if term]
    if not cleaned:
        return "general"
    return "_".join(cleaned[:2])


def _price_band(series: pd.Series) -> pd.Series:
    if series.empty:
        return pd.Series(dtype="object")
    if series.nunique() < 3:
        return pd.Series(["mid"] * len(series), index=series.index)
    q1 = series.quantile(0.33)
    q2 = series.quantile(0.66)
    return series.apply(lambda value: "low" if value <= q1 else ("mid" if value <= q2 else "high"))


def _best_text_columns(frame: pd.DataFrame) -> list[str]:
    candidates = ["title", "name", "description", "availability_text"]
    chosen = [column for column in candidates if column in frame.columns]
    if chosen:
        return chosen
    dynamic = []
    for column in frame.columns:
        if frame[column].dtype == "object" and column not in {
            "dataset_name",
            "compartment",
            "source_url",
            "source_domain",
            "schema_path",
            "scraped_at",
        }:
            dynamic.append(column)
        if len(dynamic) >= 2:
            break
    return dynamic


def enrich_with_ai(input_path: str, output_path: str | None = None) -> dict:
    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Processed CSV not found at {input_path}")

    output_target = output_path or input_path
    frame = pd.read_csv(input_path)
    if frame.empty:
        frame["ai_cluster_id"] = pd.Series(dtype="int64")
        frame["ai_cluster_label"] = pd.Series(dtype="object")
        frame["ai_price_band"] = pd.Series(dtype="object")
        frame.to_csv(output_target, index=False)
        return {"input_path": input_path, "output_path": output_target, "rows_enriched": 0, "clusters": 0}

    text_columns = _best_text_columns(frame)
    if text_columns:
        text_series = frame[text_columns].fillna("").astype(str).agg(" ".join, axis=1).str.strip()
    else:
        text_series = frame["source_url"].fillna("").astype(str)

    frame["ai_cluster_id"] = 0
    frame["ai_cluster_label"] = "general"

    unique_docs = text_series.nunique()
    if len(frame) >= 5 and unique_docs >= 3:
        cluster_count = min(5, len(frame), unique_docs)
        vectorizer = TfidfVectorizer(stop_words="english", max_features=400)
        matrix = vectorizer.fit_transform(text_series)
        model = KMeans(n_clusters=cluster_count, random_state=42, n_init=10)
        cluster_ids = model.fit_predict(matrix)
        frame["ai_cluster_id"] = cluster_ids

        features = vectorizer.get_feature_names_out()
        labels: dict[int, str] = {}
        for cluster_id in range(cluster_count):
            centroid = model.cluster_centers_[cluster_id]
            top_idx = centroid.argsort()[-3:][::-1]
            terms = [features[index] for index in top_idx if centroid[index] > 0]
            labels[cluster_id] = _cluster_label(terms)
        frame["ai_cluster_label"] = frame["ai_cluster_id"].map(labels).fillna("general")

    if "price" in frame.columns:
        prices = pd.to_numeric(frame["price"], errors="coerce").fillna(0)
        frame["ai_price_band"] = _price_band(prices)
    else:
        frame["ai_price_band"] = "n/a"

    frame.to_csv(output_target, index=False)

    return {
        "input_path": input_path,
        "output_path": output_target,
        "rows_enriched": int(len(frame)),
        "clusters": int(frame["ai_cluster_id"].nunique()),
    }
