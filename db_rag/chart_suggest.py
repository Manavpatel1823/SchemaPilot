from typing import Any, Dict, List


def _is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _looks_like_datetime_name(col_name: str) -> bool:
    name = col_name.lower()
    keywords = [
        "date",
        "time",
        "month",
        "year",
        "day",
        "created_at",
        "updated_at",
        "enrolled_at",
    ]
    return any(k in name for k in keywords)


def _looks_like_count(col_name: str) -> bool:
    name = col_name.lower()
    return any(k in name for k in ["count", "freq", "frequency", "size", "total", "number"])


def _looks_like_measure(col_name: str) -> bool:
    name = col_name.lower()
    return any(k in name for k in ["avg", "average", "score", "amount", "value", "rate"])


def _looks_like_identifier(col_name: str) -> bool:
    name = col_name.lower()
    return (
        name == "id"
        or name.endswith("_id")
        or name.startswith("id_")
    )


def _infer_column_kind(col_name: str, values: List[Any]) -> str:
    non_null = [v for v in values if v is not None]

    if not non_null:
        return "unknown"

    if _looks_like_datetime_name(col_name):
        return "datetime"

    numeric_count = sum(1 for v in non_null if _is_number(v))
    if numeric_count == len(non_null):
        return "numeric"

    unique_count = len(set(str(v) for v in non_null))
    if unique_count <= max(20, len(non_null) // 2):
        return "categorical"

    return "categorical"


def _transpose_rows(columns: List[str], rows: List[Any]) -> Dict[str, List[Any]]:
    col_values: Dict[str, List[Any]] = {c: [] for c in columns}

    for row in rows:
        if isinstance(row, dict):
            for col in columns:
                col_values[col].append(row.get(col))
        else:
            for i, col in enumerate(columns):
                value = row[i] if i < len(row) else None
                col_values[col].append(value)

    return col_values


def _pick_best_scatter_pair(numeric_cols: List[str]) -> tuple[str, str] | tuple[None, None]:
    """
    Pick a better numeric pair for scatter:
    - prefer count/size/total on x
    - prefer average/score/value on y
    - avoid raw ids if possible
    """
    if len(numeric_cols) < 2:
        return None, None

    non_id_numeric = [c for c in numeric_cols if not _looks_like_identifier(c)]

    x_candidates = [c for c in non_id_numeric if _looks_like_count(c)]
    y_candidates = [c for c in non_id_numeric if _looks_like_measure(c)]

    if x_candidates and y_candidates:
        x_col = x_candidates[0]
        y_col = y_candidates[0]
        if x_col != y_col:
            return x_col, y_col

    # fallback: any two non-id numeric columns
    if len(non_id_numeric) >= 2:
        return non_id_numeric[0], non_id_numeric[1]

    # fallback: use any two numeric columns
    return numeric_cols[0], numeric_cols[1]


def suggest_chart(columns: List[str], rows: List[Any]) -> Dict[str, Any]:
    if not columns or not rows:
        return {
            "chart_type": "table",
            "x": None,
            "y": None,
            "title": "Query Results",
            "reason": "No rows available.",
            "confidence": 1.0,
        }

    col_values = _transpose_rows(columns, rows)

    column_kinds = {
        col: _infer_column_kind(col, values)
        for col, values in col_values.items()
    }

    # -------------------------
    # Single numeric column -> histogram
    # -------------------------
    if len(columns) == 1:
        col = columns[0]

        if column_kinds[col] == "numeric" and len(rows) >= 5:
            return {
                "chart_type": "histogram",
                "x": col,
                "y": None,
                "title": f"Distribution of {col.replace('_', ' ').title()}",
                "reason": "Single numeric column distribution.",
                "confidence": 0.9,
            }

        return {
            "chart_type": "table",
            "x": None,
            "y": None,
            "title": "Query Results",
            "reason": "Single column not suitable for chart.",
            "confidence": 0.8,
        }

    numeric_cols = [c for c in columns if column_kinds[c] == "numeric"]
    categorical_cols = [c for c in columns if column_kinds[c] == "categorical"]
    datetime_cols = [c for c in columns if column_kinds[c] == "datetime"]

    # -------------------------
    # Grouped histogram: score + count
    # -------------------------
    if len(columns) == 2:
        c1, c2 = columns
        k1, k2 = column_kinds[c1], column_kinds[c2]

        if k1 == "numeric" and k2 == "numeric" and _looks_like_count(c2):
            return {
                "chart_type": "histogram",
                "x": c1,
                "y": c2,
                "title": f"Distribution of {c1.replace('_', ' ').title()}",
                "reason": "Numeric values with frequency counts.",
                "confidence": 0.95,
            }

    # -------------------------
    # Line chart: datetime + numeric
    # -------------------------
    if datetime_cols and numeric_cols:
        return {
            "chart_type": "line",
            "x": datetime_cols[0],
            "y": numeric_cols[0],
            "title": f"{numeric_cols[0].replace('_', ' ').title()} Over {datetime_cols[0].replace('_', ' ').title()}",
            "reason": "Time series trend.",
            "confidence": 0.92,
        }

    # -------------------------
    # Scatter chart: better numeric pair selection
    # -------------------------
    if len(numeric_cols) >= 2 and len(rows) >= 5:
        x_col, y_col = _pick_best_scatter_pair(numeric_cols)
        if x_col and y_col:
            return {
                "chart_type": "scatter",
                "x": x_col,
                "y": y_col,
                "title": f"{y_col.replace('_', ' ').title()} vs {x_col.replace('_', ' ').title()}",
                "reason": "Two meaningful numeric columns detected.",
                "confidence": 0.9,
            }

    # -------------------------
    # Bar chart: categorical + numeric
    # -------------------------
    if categorical_cols and numeric_cols and len(rows) <= 40:
        x_col = categorical_cols[0]

        y_candidates = [c for c in numeric_cols if not _looks_like_identifier(c)]
        y_col = y_candidates[0] if y_candidates else numeric_cols[0]

        return {
            "chart_type": "bar",
            "x": x_col,
            "y": y_col,
            "title": f"{y_col.replace('_', ' ').title()} by {x_col.replace('_', ' ').title()}",
            "reason": "Category comparison.",
            "confidence": 0.9,
        }

    return {
        "chart_type": "table",
        "x": None,
        "y": None,
        "title": "Query Results",
        "reason": "Shape not suitable for chart.",
        "confidence": 0.7,
    }