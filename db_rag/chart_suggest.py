from typing import Any, Dict, List

def suggest_chart(columns, rows):

    if not rows:
        return {"type": "table"}

    sample = rows[0]

    def is_number(v):
        return isinstance(v, (int, float)) and v is not True and v is not False

    numeric_cols = []
    categorical_cols = []

    for col in columns:
        val = sample[col]
        if is_number(val):
            numeric_cols.append(col)
        else:
            categorical_cols.append(col)

    # 1 categorical + 1 numeric → bar chart
    if len(categorical_cols) == 1 and len(numeric_cols) == 1:
        return {
            "type": "bar",
            "x": categorical_cols[0],
            "y": numeric_cols[0]
        }

    # 1 categorical + multiple numeric → grouped bar
    if len(categorical_cols) == 1 and len(numeric_cols) > 1:
        return {
            "type": "grouped_bar",
            "x": categorical_cols[0],
            "ys": numeric_cols
        }

    # 1 numeric only → histogram
    if len(numeric_cols) == 1 and len(columns) == 1:
        return {
            "type": "histogram",
            "x": numeric_cols[0],
            "bins": 10
        }

    return {"type": "table"}