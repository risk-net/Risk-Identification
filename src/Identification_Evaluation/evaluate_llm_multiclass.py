#!/usr/bin/env python3
"""Evaluate AI risk classification predictions against manual doc-level labels."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

MODULE_DIR = Path(__file__).resolve().parents[1]
PROJECT_ROOT = MODULE_DIR.parents[1]
DEFAULT_INPUT = PROJECT_ROOT / "data" / "evaluation_dataset_2000.json"
DEFAULT_OUTPUT = PROJECT_ROOT / "outputs" / "Identification_Evaluation" / "llm_multiclass" / "metrics_summary.json"
CLASSES = ["AIrisk_Irrelevant", "AIrisk_relevant_event", "AIrisk_relevant_discussion"]

DOC_LEVEL_MAPPING = {
    "事件级": "AIrisk_relevant_event",
    "讨论级": "AIrisk_relevant_discussion",
    "无关（不涉及AI风险）": "AIrisk_Irrelevant",
}

ALIASES = {
    "airisk_relevant_event": "AIrisk_relevant_event",
    "aigcrisk_relevant_event": "AIrisk_relevant_event",
    "ai_risk_relevant_event": "AIrisk_relevant_event",
    "airisk_relevant_discussion": "AIrisk_relevant_discussion",
    "aigcrisk_relevant_discussion": "AIrisk_relevant_discussion",
    "ai_risk_relevant_discussion": "AIrisk_relevant_discussion",
    "airisk_irrelevant": "AIrisk_Irrelevant",
    "aigcrisk_irrelevant": "AIrisk_Irrelevant",
    "ai_risk_irrelevant": "AIrisk_Irrelevant",
    "irrelevant": "AIrisk_Irrelevant",
    "relevant": "AIrisk_relevant_event",
    "true": "AIrisk_relevant_event",
    "false": "AIrisk_Irrelevant",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT, help="Merged evaluation dataset JSON.")
    parser.add_argument("--prediction-field", default="classification_result", help="Field containing model predictions.")
    parser.add_argument("--label-field", default="doc_level", help="Field containing manual labels.")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT, help="Optional JSON metrics output path.")
    return parser.parse_args()


def normalize_prediction(value: Any) -> str:
    if value in CLASSES:
        return str(value)
    text = str(value or "").strip()
    if text in DOC_LEVEL_MAPPING:
        return DOC_LEVEL_MAPPING[text]
    lower = text.lower()
    if lower in ALIASES:
        return ALIASES[lower]
    if "discussion" in lower:
        return "AIrisk_relevant_discussion"
    if "event" in lower:
        return "AIrisk_relevant_event"
    if "无关" in text or "irrelevant" in lower:
        return "AIrisk_Irrelevant"
    if "讨论" in text:
        return "AIrisk_relevant_discussion"
    if "事件" in text:
        return "AIrisk_relevant_event"
    return "AIrisk_Irrelevant"


def normalize_label(value: Any) -> str:
    text = str(value or "").strip()
    if text in DOC_LEVEL_MAPPING:
        return DOC_LEVEL_MAPPING[text]
    return normalize_prediction(text)


def safe_div(numerator: float, denominator: float) -> float:
    return numerator / denominator if denominator else 0.0


def build_confusion_matrix(
    records: list[dict[str, Any]], prediction_field: str, label_field: str
) -> dict[str, dict[str, int]]:
    matrix = {pred: {true: 0 for true in CLASSES} for pred in CLASSES}
    for record in records:
        pred = normalize_prediction(record.get(prediction_field))
        true = normalize_label(record.get(label_field))
        matrix[pred][true] += 1
    return matrix


def calculate_metrics(matrix: dict[str, dict[str, int]]) -> dict[str, Any]:
    total = sum(sum(row.values()) for row in matrix.values())
    correct = sum(matrix[label][label] for label in CLASSES)
    metrics: dict[str, Any] = {
        "total_samples": total,
        "correct_predictions": correct,
        "accuracy": safe_div(correct, total),
        "classes": {},
        "confusion_matrix": matrix,
    }

    for label in CLASSES:
        tp = matrix[label][label]
        fp = sum(matrix[label][other] for other in CLASSES if other != label)
        fn = sum(matrix[other][label] for other in CLASSES if other != label)
        tn = sum(
            matrix[pred][true]
            for pred in CLASSES
            for true in CLASSES
            if pred != label and true != label
        )
        precision = safe_div(tp, tp + fp)
        recall = safe_div(tp, tp + fn)
        specificity = safe_div(tn, tn + fp)
        f1 = safe_div(2 * precision * recall, precision + recall) if (precision + recall) else 0.0
        metrics["classes"][label] = {
            "tp": tp,
            "fp": fp,
            "fn": fn,
            "tn": tn,
            "precision": precision,
            "recall": recall,
            "specificity": specificity,
            "f1": f1,
        }
    return metrics


def print_report(metrics: dict[str, Any]) -> None:
    print(f"Total samples: {metrics['total_samples']}")
    print(f"Correct predictions: {metrics['correct_predictions']}")
    print(f"Accuracy: {metrics['accuracy']:.4f}\n")
    print("Confusion matrix (predicted -> true):")
    for pred in CLASSES:
        row = "  " + pred + ": " + ", ".join(
            f"{true}={metrics['confusion_matrix'][pred][true]}" for true in CLASSES
        )
        print(row)
    print("\nPer-class metrics:")
    for label in CLASSES:
        item = metrics["classes"][label]
        print(
            f"  {label}: precision={item['precision']:.4f}, recall={item['recall']:.4f}, "
            f"specificity={item['specificity']:.4f}, f1={item['f1']:.4f}"
        )


def main() -> None:
    args = parse_args()
    with args.input.open("r", encoding="utf-8") as fh:
        records = json.load(fh)
    if not isinstance(records, list):
        raise ValueError("Input JSON must contain a list of records.")

    matrix = build_confusion_matrix(records, args.prediction_field, args.label_field)
    metrics = calculate_metrics(matrix)
    print_report(metrics)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", encoding="utf-8") as fh:
        json.dump(metrics, fh, ensure_ascii=False, indent=2)
    print(f"\nSaved metrics to: {args.output}")


if __name__ == "__main__":
    main()
