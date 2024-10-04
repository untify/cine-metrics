import os
from datetime import timedelta

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from dotenv import load_dotenv
from streamlit_extras.chart_container import chart_container

import streamlit as st
from src.resources.database import MotherDuckResource

# Page configuration
st.set_page_config(page_title="CineMetrics Dashboard", page_icon="🎬", layout="wide")

# Load environment variables
load_dotenv()

# Initialize database connection
motherduck_resource = MotherDuckResource(
    connection_string=os.getenv("MOTHERDUCK_CONNECTION_STRING"), token=os.getenv("MOTHERDUCK_TOKEN")
)


# Function to load data with caching
@st.cache_data(ttl=3600)
def load_data(query: str) -> pd.DataFrame:
    with motherduck_resource.connection() as conn:
        return pd.read_sql_query(query, conn)


def main():
    st.title("🎬 CineMetrics Dashboard")

    # Fetch the latest date
    query_dates = """
        SELECT MIN(revenue_date) as earliest_date, MAX(revenue_date) as latest_date
        FROM main_marts.fct_daily_revenues
    """
    df_dates = load_data(query_dates)
    earliest_date = df_dates["earliest_date"].iloc[0]
    latest_date = df_dates["latest_date"].iloc[0]

    st.sidebar.info(
        f"Data available from {earliest_date.strftime('%Y-%m-%d')} to {latest_date.strftime('%Y-%m-%d')}"
    )

    # Section 1: Box Office Overview
    st.header("📊 Box Office Rankings")

    col1, col2 = st.columns(2)
    with col1:
        period = st.selectbox(
            "Select period",
            ["Last Week", "Last Month", "Last Year", "All Time"],
            key="period_select",
        )
    with col2:
        display_type = st.radio("Display Type", ["Top 10", "Top 20"])

    limit = 10 if display_type == "Top 10" else 20

    if period == "Last Week":
        start_date = latest_date - timedelta(days=7)
        query = f"""
            SELECT m.title, SUM(f.revenue) as total_revenue
            FROM main_marts.fct_daily_revenues f
            JOIN main_marts.dim_movies m ON f.movie_key = m.movie_key
            JOIN main_marts.dim_dates d ON f.date_key = d.date_key
            WHERE d.date BETWEEN '{start_date}'::DATE AND '{latest_date}'::DATE
            GROUP BY m.title
            ORDER BY total_revenue DESC
            LIMIT {limit}
        """
        title = f"Top {limit} Movies by Revenue - Last Week ({start_date.strftime('%Y-%m-%d')} to {latest_date.strftime('%Y-%m-%d')})"
    elif period == "Last Month":
        start_date = latest_date.replace(day=1)
        query = f"""
            SELECT m.title, SUM(f.revenue) as total_revenue
            FROM main_marts.fct_daily_revenues f
            JOIN main_marts.dim_movies m ON f.movie_key = m.movie_key
            JOIN main_marts.dim_dates d ON f.date_key = d.date_key
            WHERE d.date BETWEEN '{start_date}'::DATE AND '{latest_date}'::DATE
            GROUP BY m.title
            ORDER BY total_revenue DESC
            LIMIT {limit}
        """
        title = f"Top {limit} Movies by Revenue - Last Month ({start_date.strftime('%B %Y')})"
    elif period == "Last Year":
        start_date = latest_date.replace(year=latest_date.year - 1)
        query = f"""
            SELECT m.title, SUM(f.revenue) as total_revenue
            FROM main_marts.fct_daily_revenues f
            JOIN main_marts.dim_movies m ON f.movie_key = m.movie_key
            JOIN main_marts.dim_dates d ON f.date_key = d.date_key
            WHERE d.date BETWEEN '{start_date}'::DATE AND '{latest_date}'::DATE
            GROUP BY m.title
            ORDER BY total_revenue DESC
            LIMIT {limit}
        """
        title = f"Top {limit} Movies by Revenue - Last Year ({start_date.strftime('%Y')})"
    else:
        query = f"""
            SELECT m.title, SUM(f.revenue) as total_revenue
            FROM main_marts.fct_daily_revenues f
            JOIN main_marts.dim_movies m ON f.movie_key = m.movie_key
            GROUP BY m.title
            ORDER BY total_revenue DESC
            LIMIT {limit}
        """
        title = f"Top {limit} Movies by Revenue - All Time"

    df_top_movies = load_data(query)

    with chart_container(df_top_movies):
        fig = px.bar(
            df_top_movies,
            x="total_revenue",
            y="title",
            orientation="h",
            title=title,
            labels={"total_revenue": "Revenue ($)", "title": "Movie Title"},
            text="total_revenue",
        )
        fig.update_traces(texttemplate="%{text:.2s}", textposition="outside")
        fig.update_layout(height=600)
        st.plotly_chart(fig, use_container_width=True)

    # Section 2: Trend Analysis
    st.header("📈 Trend Analysis")

    trend_period = st.selectbox(
        "Select trend analysis period",
        ["Last 30 days", "Last 90 days", "Last year", "All time"],
        key="trend_period_select",
    )

    distributor_category = st.selectbox(
        "Select distributor category",
        ["All", "Major", "Medium", "Minor"],
        key="distributor_category_select",
    )

    if trend_period == "Last 30 days":
        start_date = latest_date - timedelta(days=30)
    elif trend_period == "Last 90 days":
        start_date = latest_date - timedelta(days=90)
    elif trend_period == "Last year":
        start_date = latest_date - timedelta(days=365)
    else:
        start_date = None

    date_condition = f"WHERE d.date >= '{start_date}'::DATE" if start_date else ""
    category_condition = (
        f"AND dist.distributor_category = '{distributor_category}'"
        if distributor_category != "All"
        else ""
    )

    query = f"""
        SELECT d.date, SUM(f.revenue) as daily_revenue, AVG(f.revenue_per_theater) as avg_revenue_per_theater
        FROM main_marts.fct_daily_revenues f
        JOIN main_marts.dim_dates d ON f.date_key = d.date_key
        JOIN main_marts.dim_distributors dist ON f.distributor_key = dist.distributor_key
        {date_condition}
        {category_condition}
        GROUP BY d.date
        ORDER BY d.date
    """
    df_trend = load_data(query)

    with chart_container(df_trend):
        fig = go.Figure()
        fig.add_trace(
            go.Scatter(
                x=df_trend["date"], y=df_trend["daily_revenue"], name="Daily Revenue", yaxis="y1"
            )
        )
        fig.add_trace(
            go.Scatter(
                x=df_trend["date"],
                y=df_trend["avg_revenue_per_theater"],
                name="Avg Revenue per Theater",
                yaxis="y2",
            )
        )
        fig.update_layout(
            title="Revenue Trend and Average Revenue per Theater",
            yaxis=dict(title="Daily Revenue ($)"),
            yaxis2=dict(title="Avg Revenue per Theater ($)", overlaying="y", side="right"),
            height=500,
        )
        st.plotly_chart(fig, use_container_width=True)

    # Section 3: Distributor Analysis
    st.header("🏢 Distributor Analysis")

    col1, col2 = st.columns(2)

    with col1:
        query = """
        SELECT dist.distributor, SUM(f.revenue) as total_revenue, COUNT(DISTINCT m.movie_key) as movie_count
        FROM main_marts.fct_daily_revenues f
        JOIN main_marts.dim_distributors dist ON f.distributor_key = dist.distributor_key
        JOIN main_marts.dim_movies m ON f.movie_key = m.movie_key
        GROUP BY dist.distributor
        ORDER BY total_revenue DESC
        LIMIT 10
        """
        df_distributors = load_data(query)

        with chart_container(df_distributors):
            fig = px.pie(
                df_distributors,
                values="total_revenue",
                names="distributor",
                title="Distributors' Share in Revenue",
                hover_data=["movie_count"],
            )
            fig.update_traces(textposition="inside", textinfo="percent+label")
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        fig = px.scatter(
            df_distributors,
            x="movie_count",
            y="total_revenue",
            size="total_revenue",
            color="distributor",
            hover_name="distributor",
            log_x=True,
            size_max=60,
            title="Distributor Revenue vs. Number of Movies",
        )
        fig.update_layout(height=500)
        st.plotly_chart(fig, use_container_width=True)

    # Section 4: Detailed Distributor Analysis
    st.header("🏢 Detailed Distributor Analysis")

    query = """
    SELECT distributor, total_movies, first_appearance_date, last_appearance_date,
           total_revenue, avg_revenue_per_movie, distributor_category
    FROM main_marts.dim_distributors
    ORDER BY total_revenue DESC
    LIMIT 20
    """
    df_distributor_details = load_data(query)

    st.dataframe(
        df_distributor_details.style.format(
            {
                "total_revenue": "${:,.0f}",
                "avg_revenue_per_movie": "${:,.0f}",
                "first_appearance_date": "{:%Y-%m-%d}",
                "last_appearance_date": "{:%Y-%m-%d}",
            }
        )
    )

    fig = px.scatter(
        df_distributor_details,
        x="total_movies",
        y="avg_revenue_per_movie",
        size="total_revenue",
        color="distributor_category",
        hover_name="distributor",
        log_x=True,
        log_y=True,
        title="Distributor Performance Analysis",
    )
    st.plotly_chart(fig, use_container_width=True)

    # Section 5: Single Movie Analysis
    st.header("🎥 Single Movie Analysis")

    query = "SELECT DISTINCT title FROM main_marts.dim_movies ORDER BY title"
    df_movies = load_data(query)

    selected_movie = st.selectbox("Select a movie", df_movies["title"], key="movie_select")

    query = f"""
    SELECT m.title, m.year, m.director, m.imdb_rating, m.released_date,
           SUM(f.revenue) as total_revenue, AVG(f.theaters) as avg_theaters,
           MAX(f.revenue) as max_daily_revenue
    FROM main_marts.fct_daily_revenues f
    JOIN main_marts.dim_movies m ON f.movie_key = m.movie_key
    WHERE m.title = '{selected_movie}'
    GROUP BY m.title, m.year, m.director, m.imdb_rating, m.released_date
    """
    df_movie_details = load_data(query)

    if not df_movie_details.empty:
        movie = df_movie_details.iloc[0]
        col1, col2, col3 = st.columns(3)
        col1.metric(
            "Total Revenue",
            f"${movie['total_revenue']:,.0f}" if pd.notnull(movie["total_revenue"]) else "N/A",
        )
        col2.metric(
            "Average Theater Count",
            f"{movie['avg_theaters']:.0f}" if pd.notnull(movie["avg_theaters"]) else "N/A",
        )
        col3.metric(
            "Max Daily Revenue",
            (
                f"${movie['max_daily_revenue']:,.0f}"
                if pd.notnull(movie["max_daily_revenue"])
                else "N/A"
            ),
        )

        st.markdown(f"**Title:** {movie['title'] if pd.notnull(movie['title']) else 'N/A'}")
        st.markdown(f"**Year:** {movie['year'] if pd.notnull(movie['year']) else 'N/A'}")
        st.markdown(
            f"**Director:** {movie['director'] if pd.notnull(movie['director']) else 'N/A'}"
        )
        st.markdown(
            f"**IMDB Rating:** {movie['imdb_rating'] if pd.notnull(movie['imdb_rating']) else 'N/A'}"
        )
        st.markdown(
            f"**Release Date:** {movie['released_date'].strftime('%Y-%m-%d') if pd.notnull(movie['released_date']) else 'N/A'}"
        )

        query = f"""
        SELECT d.date, f.revenue, f.theaters, f.revenue_per_theater
        FROM main_marts.fct_daily_revenues f
        JOIN main_marts.dim_dates d ON f.date_key = d.date_key
        JOIN main_marts.dim_movies m ON f.movie_key = m.movie_key
        WHERE m.title = '{selected_movie}'
        ORDER BY d.date
        """
        df_movie_revenue = load_data(query)

        fig = go.Figure()
        fig.add_trace(
            go.Scatter(
                x=df_movie_revenue["date"],
                y=df_movie_revenue["revenue"],
                name="Daily Revenue",
                yaxis="y1",
            )
        )
        fig.add_trace(
            go.Scatter(
                x=df_movie_revenue["date"],
                y=df_movie_revenue["theaters"],
                name="Theater Count",
                yaxis="y2",
            )
        )
        fig.update_layout(
            title=f"Daily Revenue and Theater Count for {selected_movie}",
            yaxis=dict(title="Revenue ($)"),
            yaxis2=dict(title="Theater Count", overlaying="y", side="right"),
            height=500,
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.write("No data available for this movie.")

    # Section 6: Weekly Analysis
    st.header("📅 Weekly Analysis")

    selected_movie_weekly = st.selectbox(
        "Select a movie for weekly analysis", df_movies["title"], key="weekly_movie_select"
    )

    query = f"""
    SELECT m.title, wr.year, wr.week_of_year_iso, wr.weekly_revenue, wr.weekly_theaters,
           wr.week_start_date, wr.week_end_date, wr.revenue_change_percentage, wr.drop_percentage,
           wr.run_stage, wr.performance_category, wr.cumulative_revenue
    FROM main_marts.fct_weekly_revenues wr
    JOIN main_marts.dim_movies m ON wr.movie_key = m.movie_key
    WHERE m.title = '{selected_movie_weekly}'
    ORDER BY wr.year, wr.week_of_year_iso
    """
    df_weekly_revenue = load_data(query)

    if not df_weekly_revenue.empty:
        fig = go.Figure()
        fig.add_trace(
            go.Bar(
                x=df_weekly_revenue["week_start_date"],
                y=df_weekly_revenue["weekly_revenue"],
                name="Weekly Revenue",
            )
        )
        fig.add_trace(
            go.Scatter(
                x=df_weekly_revenue["week_start_date"],
                y=df_weekly_revenue["weekly_theaters"],
                name="Theater Count",
                yaxis="y2",
            )
        )
        fig.update_layout(
            title=f"Weekly Revenue and Theater Count for {selected_movie_weekly}",
            xaxis_title="Week",
            yaxis=dict(title="Revenue ($)"),
            yaxis2=dict(title="Theater Count", overlaying="y", side="right"),
            height=500,
        )
        st.plotly_chart(fig, use_container_width=True)

        st.write("### Detailed Weekly Data:")
        st.dataframe(
            df_weekly_revenue.style.format(
                {
                    "weekly_revenue": "${:,.0f}",
                    "weekly_theaters": "{:,.0f}",
                    "revenue_change_percentage": "{:.2%}",
                    "drop_percentage": "{:.2%}",
                    "cumulative_revenue": "${:,.0f}",
                }
            )
        )

        # Additional chart for weekly analysis
        fig = px.line(
            df_weekly_revenue,
            x="week_start_date",
            y="revenue_change_percentage",
            title=f"Weekly Revenue Change Percentage for {selected_movie_weekly}",
            labels={"revenue_change_percentage": "Revenue Change (%)", "week_start_date": "Date"},
            color="run_stage",
            hover_data=["performance_category"],
        )
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)

        # New chart for cumulative revenue
        fig = px.line(
            df_weekly_revenue,
            x="week_start_date",
            y="cumulative_revenue",
            title=f"Cumulative Revenue for {selected_movie_weekly}",
            labels={"cumulative_revenue": "Cumulative Revenue ($)", "week_start_date": "Date"},
        )
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)

    else:
        st.write("No weekly data available for this movie.")


if __name__ == "__main__":
    main()
