from __future__ import annotations

from datetime import datetime

import pandas as pd
import plotly.express as px
import streamlit as st

from snowflake_connection import run_query

st.set_page_config(page_title="Travel Intelligence Platform", layout="wide")

BLUE_SCALE = px.colors.sequential.Blues


def render_revenue_page() -> None:
    st.title("Revenue Overview")

    monthly_revenue_query = """
        SELECT
            DATE_TRUNC('month', trip_start_date) AS month,
            SUM(premium_amount) AS total_premium,
            COUNT(*) AS policy_count
        FROM GOLD.fact_policies
        GROUP BY 1
        ORDER BY 1
    """

    destination_premium_query = """
        SELECT
            d.destination_country,
            SUM(f.premium_amount) AS total_premium
        FROM GOLD.fact_policies f
        LEFT JOIN GOLD.dim_destinations d
            ON f.destination_sk = d.destination_sk
        GROUP BY 1
        ORDER BY 2 DESC
    """

    monthly_df = run_query(monthly_revenue_query)
    destination_df = run_query(destination_premium_query)

    if monthly_df.empty:
        st.warning("No policy revenue data available yet.")
        return

    monthly_df["MONTH"] = pd.to_datetime(monthly_df["MONTH"])
    current_year = datetime.now().year
    current_year_df = monthly_df[monthly_df["MONTH"].dt.year == current_year]

    total_revenue_year = float(current_year_df["TOTAL_PREMIUM"].sum())
    total_policies = int(monthly_df["POLICY_COUNT"].sum())
    avg_premium = (total_revenue_year / total_policies) if total_policies else 0.0

    metric_cols = st.columns(3)
    metric_cols[0].metric("Total Revenue This Year", f"${total_revenue_year:,.2f}")
    metric_cols[1].metric("Total Policies Sold", f"{total_policies:,}")
    metric_cols[2].metric("Average Premium", f"${avg_premium:,.2f}")

    trend_chart = px.line(
        monthly_df,
        x="MONTH",
        y="TOTAL_PREMIUM",
        markers=True,
        title="Monthly Premium Revenue Trend",
        color_discrete_sequence=[BLUE_SCALE[6]],
    )
    st.plotly_chart(trend_chart, use_container_width=True)

    if not destination_df.empty:
        destination_bar = px.bar(
            destination_df,
            x="DESTINATION_COUNTRY",
            y="TOTAL_PREMIUM",
            title="Premium by Destination Country",
            color="TOTAL_PREMIUM",
            color_continuous_scale=BLUE_SCALE,
        )
        destination_bar.update_layout(xaxis_title="Destination Country", yaxis_title="Total Premium")
        st.plotly_chart(destination_bar, use_container_width=True)


def render_claims_page() -> None:
    st.title("Claims Analysis")

    claims_metrics_query = """
        SELECT
            COUNT(*) AS total_claims,
            SUM(CASE WHEN is_approved THEN 1 ELSE 0 END) AS approved_claims,
            ROUND((SUM(CASE WHEN is_approved THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0)) * 100, 2) AS approval_rate
        FROM GOLD.fact_claims
    """

    claims_type_query = """
        SELECT
            claim_type,
            COUNT(*) AS claim_count
        FROM GOLD.fact_claims
        GROUP BY 1
        ORDER BY 2 DESC
    """

    destination_claim_query = """
        SELECT
            d.destination_country,
            AVG(f.claim_amount) AS avg_claim_amount
        FROM GOLD.fact_claims f
        LEFT JOIN GOLD.dim_destinations d
            ON f.destination_sk = d.destination_sk
        GROUP BY 1
        ORDER BY 2 DESC
    """

    approval_comparison_query = """
        SELECT
            claim_type,
            claim_status,
            COUNT(*) AS claim_count
        FROM GOLD.fact_claims
        WHERE claim_status IN ('approved', 'rejected')
        GROUP BY 1, 2
        ORDER BY 1, 2
    """

    recent_claims_query = """
        SELECT
            claim_date,
            destination_country,
            claim_type,
            claim_amount,
            claim_status AS status
        FROM GOLD.fact_claims
        ORDER BY claim_date DESC
        LIMIT 20
    """

    metrics_df = run_query(claims_metrics_query)
    claims_type_df = run_query(claims_type_query)
    destination_claim_df = run_query(destination_claim_query)
    approval_df = run_query(approval_comparison_query)
    recent_claims_df = run_query(recent_claims_query)

    if metrics_df.empty:
        st.warning("No claims data available yet.")
        return

    total_claims = int(metrics_df.iloc[0]["TOTAL_CLAIMS"] or 0)
    approved_claims = int(metrics_df.iloc[0]["APPROVED_CLAIMS"] or 0)
    approval_rate = float(metrics_df.iloc[0]["APPROVAL_RATE"] or 0)

    metric_cols = st.columns(3)
    metric_cols[0].metric("Total Claims Filed", f"{total_claims:,}")
    metric_cols[1].metric("Total Claims Approved", f"{approved_claims:,}")
    metric_cols[2].metric("Overall Approval Rate", f"{approval_rate:.2f}%")

    if not claims_type_df.empty:
        pie = px.pie(
            claims_type_df,
            names="CLAIM_TYPE",
            values="CLAIM_COUNT",
            title="Claim Count by Claim Type",
            color_discrete_sequence=BLUE_SCALE,
        )
        st.plotly_chart(pie, use_container_width=True)

    chart_cols = st.columns(2)

    if not destination_claim_df.empty:
        avg_claim_bar = px.bar(
            destination_claim_df,
            x="DESTINATION_COUNTRY",
            y="AVG_CLAIM_AMOUNT",
            title="Average Claim Amount by Destination",
            color="AVG_CLAIM_AMOUNT",
            color_continuous_scale=BLUE_SCALE,
        )
        avg_claim_bar.update_layout(xaxis_title="Destination Country", yaxis_title="Average Claim Amount")
        chart_cols[0].plotly_chart(avg_claim_bar, use_container_width=True)

    if not approval_df.empty:
        grouped = px.bar(
            approval_df,
            x="CLAIM_TYPE",
            y="CLAIM_COUNT",
            color="CLAIM_STATUS",
            barmode="group",
            title="Approved vs Rejected Claims by Type",
            color_discrete_sequence=[BLUE_SCALE[4], BLUE_SCALE[7]],
        )
        chart_cols[1].plotly_chart(grouped, use_container_width=True)

    st.subheader("20 Most Recent Claims")
    st.dataframe(recent_claims_df, use_container_width=True)


def render_broker_page() -> None:
    st.title("Broker Performance")

    brokers_query = """
        SELECT
            broker_id,
            broker_name,
            broker_company,
            country,
            policies_sold_today,
            total_premium_value,
            commission_rate,
            commission_percentage,
            commission_earned,
            report_date
        FROM GOLD.dim_brokers
        ORDER BY commission_earned DESC
    """

    brokers_df = run_query(brokers_query)
    if brokers_df.empty:
        st.warning("No broker data available yet.")
        return

    countries = ["All"] + sorted([country for country in brokers_df["COUNTRY"].dropna().unique()])
    selected_country = st.sidebar.selectbox("Filter Broker Country", countries)

    filtered_df = brokers_df if selected_country == "All" else brokers_df[brokers_df["COUNTRY"] == selected_country]

    st.dataframe(
        filtered_df,
        use_container_width=True,
        column_config={
            "COMMISSION_EARNED": st.column_config.NumberColumn(
                "COMMISSION_EARNED",
                format="$%.2f",
            ),
            "TOTAL_PREMIUM_VALUE": st.column_config.NumberColumn(
                "TOTAL_PREMIUM_VALUE",
                format="$%.2f",
            ),
        },
    )

    top_15 = filtered_df.head(15)
    top_brokers_chart = px.bar(
        top_15.sort_values("COMMISSION_EARNED", ascending=True),
        x="COMMISSION_EARNED",
        y="BROKER_NAME",
        orientation="h",
        title="Top 15 Brokers by Commission Earned",
        color="COMMISSION_EARNED",
        color_continuous_scale=BLUE_SCALE,
    )
    st.plotly_chart(top_brokers_chart, use_container_width=True)

    scatter = px.scatter(
        filtered_df,
        x="POLICIES_SOLD_TODAY",
        y="COMMISSION_EARNED",
        color="COUNTRY",
        hover_name="BROKER_NAME",
        title="Policies Sold vs Commission Earned",
    )
    st.plotly_chart(scatter, use_container_width=True)


def render_flight_page() -> None:
    st.title("Flight Intelligence")

    flight_stats_query = """
        SELECT
            origin_country,
            COUNT(*) AS flight_count,
            AVG(altitude) AS avg_altitude,
            AVG(velocity) AS avg_velocity,
            SUM(CASE WHEN on_ground THEN 1 ELSE 0 END) AS ground_count
        FROM SILVER.stg_flights
        GROUP BY origin_country
        ORDER BY flight_count DESC
    """

    latest_flights_query = """
        SELECT
            icao24,
            callsign,
            origin_country,
            altitude,
            velocity,
            on_ground,
            ingested_at
        FROM BRONZE.RAW_FLIGHTS
        ORDER BY ingested_at DESC
        LIMIT 50
    """

    stats_df = run_query(flight_stats_query)
    latest_df = run_query(latest_flights_query)

    if stats_df.empty:
        st.warning("No flight data available yet.")
        return

    total_flights = int(stats_df["FLIGHT_COUNT"].sum())
    st.metric("Total Live Flights Tracked", f"{total_flights:,}")

    map_chart = px.choropleth(
        stats_df,
        locations="ORIGIN_COUNTRY",
        locationmode="country names",
        color="FLIGHT_COUNT",
        color_continuous_scale=BLUE_SCALE,
        projection="natural earth",
        title="Flight Volume by Origin Country",
    )
    st.plotly_chart(map_chart, use_container_width=True)

    top_20 = stats_df.head(20)
    top_country_chart = px.bar(
        top_20,
        x="ORIGIN_COUNTRY",
        y="FLIGHT_COUNT",
        title="Top 20 Countries by Flight Volume",
        color="FLIGHT_COUNT",
        color_continuous_scale=BLUE_SCALE,
    )
    st.plotly_chart(top_country_chart, use_container_width=True)

    st.subheader("Latest 50 Flight Events")
    st.dataframe(latest_df, use_container_width=True)


def main() -> None:
    st.sidebar.title("Travel Intelligence Platform")
    page = st.sidebar.radio(
        "Navigation",
        ["Revenue Overview", "Claims Analysis", "Broker Performance", "Flight Intelligence"],
    )
    st.sidebar.caption(f"Last refreshed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    if page == "Revenue Overview":
        render_revenue_page()
    elif page == "Claims Analysis":
        render_claims_page()
    elif page == "Broker Performance":
        render_broker_page()
    elif page == "Flight Intelligence":
        render_flight_page()


if __name__ == "__main__":
    main()
