import time
from pathlib import Path

import pandas as pd
import streamlit as st

st.set_page_config(page_title="Cloud Pipeline Metrics", layout="wide")
st.title("Cloud Data Pipeline Live Metrics")

KAFKA_PATH = Path("metrics/kafka_metrics/kafka_input_metrics.csv")
SPARK_S3_PATH = "s3://fraud-data-lake-varun/metrics/spark_delay_v2/"

REFRESH_SECONDS = 5


def load_kafka_metrics() -> pd.DataFrame | None:
    if not KAFKA_PATH.exists() or KAFKA_PATH.stat().st_size == 0:
        return None

    try:
        df = pd.read_csv(KAFKA_PATH)
    except Exception:
        return None

    if df.empty:
        return None

    df["window_start_utc"] = pd.to_datetime(df["window_start_utc"], utc=True)
    df["elapsed_sec"] = (
        df["window_start_utc"] - df["window_start_utc"].iloc[0]
    ).dt.total_seconds()

    return df


@st.cache_data(ttl=5)
def load_spark_metrics() -> pd.DataFrame | None:
    try:
        df = pd.read_parquet(SPARK_S3_PATH)
    except Exception:
        return None

    if df.empty:
        return None

    df["start_window"] = pd.to_datetime(df["start_window"], utc=True)
    df["end_window"] = pd.to_datetime(df["end_window"], utc=True)

    df = df.sort_values("start_window")

    df["elapsed_sec"] = (
        df["start_window"] - df["start_window"].iloc[0]
    ).dt.total_seconds()

    return df


kafka_df = load_kafka_metrics()
spark_df = load_spark_metrics()

top_left, top_mid, top_right, top_far_right = st.columns(4)

with top_left:
    if kafka_df is not None:
        st.metric(
            "Current Kafka Input Rate",
            f"{kafka_df['msg_per_sec'].iloc[-1]:.2f} msg/s"
        )
    else:
        st.metric("Current Kafka Input Rate", "No data")

with top_mid:
    if spark_df is not None:
        st.metric(
            "Current Avg Delay",
            f"{spark_df['avg_delay_sec'].iloc[-1]:.2f} s"
        )
    else:
        st.metric("Current Avg Delay", "No data")

with top_right:
    if spark_df is not None:
        st.metric(
            "Current Max Delay",
            f"{spark_df['max_delay_sec'].iloc[-1]:.2f} s"
        )
    else:
        st.metric("Current Max Delay", "No data")

with top_far_right:
    if spark_df is not None:
        st.metric(
            "Records Processed",
            f"{int(spark_df['records_processed'].iloc[-1])}"
        )
    else:
        st.metric("Records Processed", "No data")

col1, col2 = st.columns(2)

with col1:
    st.subheader("Kafka Input Rate")
    if kafka_df is not None:
        kafka_chart = kafka_df[["elapsed_sec", "msg_per_sec"]].copy()
        kafka_chart = kafka_chart.set_index("elapsed_sec")
        st.line_chart(kafka_chart, use_container_width=True)

        with st.expander("Kafka Metrics Table"):
            st.dataframe(kafka_df.tail(15), use_container_width=True)
    else:
        st.info("Waiting for Kafka metrics...")

with col2:
    st.subheader("Spark Processing Delay")
    if spark_df is not None:
        spark_chart = spark_df[["elapsed_sec", "avg_delay_sec", "max_delay_sec"]].copy()
        spark_chart = spark_chart.set_index("elapsed_sec")
        st.line_chart(spark_chart, use_container_width=True)

        with st.expander("Spark Metrics Table"):
            st.dataframe(
                spark_df[
                    [
                        "start_window",
                        "end_window",
                        "records_processed",
                        "avg_delay_sec",
                        "max_delay_sec",
                        "suspicious_count",
                        "total_amount",
                    ]
                ].tail(15),
                use_container_width=True,
            )
    else:
        st.info("Waiting for Spark metrics...")

st.divider()

bottom_left, bottom_right = st.columns(2)

with bottom_left:
    st.subheader("Records Processed Over Time")
    if spark_df is not None:
        processed_chart = spark_df[["elapsed_sec", "records_processed"]].copy()
        processed_chart = processed_chart.set_index("elapsed_sec")
        st.line_chart(processed_chart, use_container_width=True)
    else:
        st.info("Waiting for records processed metrics...")

with bottom_right:
    st.subheader("Suspicious Transactions Over Time")
    if spark_df is not None:
        suspicious_chart = spark_df[["elapsed_sec", "suspicious_count"]].copy()
        suspicious_chart = suspicious_chart.set_index("elapsed_sec")
        st.line_chart(suspicious_chart, use_container_width=True)
    else:
        st.info("Waiting for suspicious transaction metrics...")

st.divider()

summary_left, summary_right = st.columns(2)

with summary_left:
    st.subheader("Kafka Summary")
    if kafka_df is not None:
        st.write(f"Average Kafka input rate: **{kafka_df['msg_per_sec'].mean():.2f} msg/s**")
        st.write(f"Peak Kafka input rate: **{kafka_df['msg_per_sec'].max():.2f} msg/s**")
        st.write(f"Samples collected: **{len(kafka_df)}**")
    else:
        st.write("No Kafka summary available yet.")

with summary_right:
    st.subheader("Spark Summary")
    if spark_df is not None:
        st.write(f"Average processing delay: **{spark_df['avg_delay_sec'].mean():.2f} s**")
        st.write(f"Peak max delay: **{spark_df['max_delay_sec'].max():.2f} s**")
        st.write(f"Average records processed: **{spark_df['records_processed'].mean():.2f}**")
        st.write(f"Total suspicious transactions: **{int(spark_df['suspicious_count'].sum())}**")
        st.write(f"Windows collected: **{len(spark_df)}**")
    else:
        st.write("No Spark summary available yet.")

st.caption(f"Refreshing every {REFRESH_SECONDS} seconds.")

time.sleep(REFRESH_SECONDS)
st.rerun()