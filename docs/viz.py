import altair as alt
import pandas as pd


def visualize_user_summary(data_file="user_summary.csv"):
    source = pd.read_csv(data_file)
    points = (
        alt.Chart(
            source,
            title="Total Spending and Items Bought by Age Group",
            width=600,
        )
        .mark_circle(size=120)
        .encode(
            x=alt.X(
                "total_spent:Q",
                axis=alt.Axis(format=".2s", title="Total Amount Spent"),
            ).scale(type="log"),
            y=alt.Y("total_items:Q", axis=alt.Axis(title="Total Items Bought")),
            color=alt.Color("binned_age:O").scale(scheme="lightgreyred"),
            tooltip=["first_name", "age", "city", "total_spent", "total_items"],
        )
        .transform_bin("binned_age", "age", bin=alt.Bin(maxbins=5))
    )
    text = points.mark_text(
        align="center",
        baseline="bottom",
        yOffset=-5,
    ).encode(text="first_name", color=alt.value("black"))

    return points + text


def visualize_category_summary(data_file="category_summary.csv"):
    source = pd.read_csv(data_file)
    total_sales = (
        alt.Chart(source)
        .encode(
            y=alt.Y(
                "total_sales:Q",
                axis=alt.Axis(format=".2s", title="Total Value of Sales"),
            ),
            x=alt.X("category:N"),
            color=alt.Color("category:N", legend=None),
            tooltip=["category", "total_sales", "items_sold"],
        )
        .mark_bar()
    )

    items_sold = (
        alt.Chart(source)
        .encode(
            y=alt.Y("items_sold:Q", axis=alt.Axis(title="Count of sold")),
            x=alt.X("category:N", axis=alt.Axis(title="Category")),
            tooltip=["category", "total_sales:Q", "items_sold"],
        )
        .mark_line(point=True)
    )
    return (
        (total_sales + items_sold)
        .resolve_scale(y="independent")
        .properties(
            title="Value of Sales and Number of Items Sold by Category",
            width=600,
        )
    )
